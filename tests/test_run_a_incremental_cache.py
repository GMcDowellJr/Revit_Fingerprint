from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import pytest

from tools import extractor, run_extract_all
from tools import run_a_cache
from tools import run_a_incremental


# A minimal, real (not synthetic-shaped) slice of the production units policy pair
# -- required/optional/allowed items copied verbatim from policies/domain_*.json
# so these tests exercise the real sig_hash + join_key computation path, not a
# toy schema of our own invention.
_UNITS_SIG_HASH_POLICY = {
    "domains": {
        "units": {
            "allowed_item_prefixes": [],
            "allowed_items": [
                "units.spec", "units.unit_type_id", "units.symbol_type_id",
                "units.accuracy", "units.rounding_method", "units.use_default",
                "units.use_digit_grouping", "units.use_plus_prefix",
                "units.suppress_leading_zeros", "units.suppress_spaces",
                "units.suppress_trailing_zeros",
            ],
            "hash_alg": "md5_utf8_join_pipe",
            "minima": {"block_if_any_required_not_ok": True},
            "required_items": ["units.spec", "units.unit_type_id"],
            "sig_hash_schema": "units.sig_hash.v1",
        }
    }
}
_UNITS_JOIN_POLICY = {
    "domains": {
        "units": {
            "join_key_schema": "units.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "required_items": ["units.spec", "units.unit_type_id", "units.rounding_method"],
            "optional_items": ["units.accuracy"],
            "explicitly_excluded_items": [],
        }
    }
}


def _units_export(file_stem: str, accuracy: str = "0.01") -> Dict:
    return {
        "_contract": {"domains": {"units": {"domain": "units", "status": "ok", "diag": {"count": 1}}}},
        "units": {
            "records": [
                {
                    "record_id": f"{file_stem}-units-1",
                    "status": "ok",
                    "identity_quality": "complete",
                    "sig_hash": "deadbeefdeadbeefdeadbeefdeadbeef",
                    "is_purgeable": False,
                    "instance_count": 1,
                    "label": {"display": "Length", "quality": "ok", "provenance": "extractor", "components": {"a": "1"}},
                    "items": [
                        {"k": "units.spec", "v": "Length", "q": "ok"},
                        {"k": "units.unit_type_id", "v": "UT_Length", "q": "ok"},
                        {"k": "units.rounding_method", "v": "0.01", "q": "ok"},
                        {"k": "units.accuracy", "v": accuracy, "q": "ok"},
                        {"k": "units.symbol_type_id", "v": "ST_None", "q": "ok"},
                    ],
                }
            ]
        },
    }


def _write_corpus(exports_dir: Path, n: int = 3) -> None:
    exports_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        payload = _units_export(f"file{i}", accuracy=str(0.01 * (i + 1)))
        (exports_dir / f"fp__proj__{i:03d}__fingerprint.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_csv(path: Path, exclude=("exported_utc",)) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return sorted(
        [{k: v for k, v in row.items() if k not in exclude} for row in rows],
        key=lambda r: tuple(sorted(r.items())),
    )


def _run_incremental(tmp_path: Path, exports_dir: Path, results_root: Path, sig_pol: Path, join_pol: Path, force_full: bool = False, sig_hash_domains=None):
    records_dir = results_root / "records"
    cache_dir = run_a_cache.cache_root(results_root)
    report = run_a_incremental.run_incremental(
        exports_dir, records_dir,
        cache_dir=cache_dir,
        sig_hash_policy_path=sig_pol,
        join_policy_path=join_pol,
        force_full=force_full,
        sig_hash_domains=sig_hash_domains,
    )
    run_a_cache.finalize_manifest(report["run_state"])
    return report, records_dir


def test_zero_change_second_run_reuses_all_files_byte_identical(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 3)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    results_root = tmp_path / "results"
    report1, records_dir = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report1["files_recomputed"] == 3
    assert report1["files_reused"] == 0

    before_records = _load_csv(records_dir / "records.csv")
    before_meta = _load_csv(records_dir / "file_metadata.csv")
    before_basis = _load_csv(records_dir / "sig_basis_items.csv", exclude=())

    report2, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report2["files_reused"] == 3
    assert report2["files_recomputed"] == 0
    assert report2["cache_was_valid"] is True

    after_records = _load_csv(records_dir / "records.csv")
    after_meta = _load_csv(records_dir / "file_metadata.csv")
    after_basis = _load_csv(records_dir / "sig_basis_items.csv", exclude=())

    assert before_records == after_records
    assert before_meta == after_meta
    assert before_basis == after_basis
    # sig_hash diagnostics are a full re-sum over cached + fresh facts every run,
    # not a delta -- must be identical when nothing changed.
    assert report1["sig_hash_diag"]["records_processed"] == report2["sig_hash_diag"]["records_processed"]
    assert report1["sig_hash_diag"]["records_hashed"] == report2["sig_hash_diag"]["records_hashed"]


def test_single_file_change_only_that_files_rows_differ(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 3)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    results_root = tmp_path / "results"
    _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    records_dir = results_root / "records"
    before = {r["record_pk"]: r for r in _load_csv(records_dir / "records.csv")}

    mutated = exports / "fp__proj__001__fingerprint.json"
    data = json.loads(mutated.read_text(encoding="utf-8"))
    data["units"]["records"][0]["items"][3]["v"] = "0.99"  # units.accuracy
    mutated.write_text(json.dumps(data), encoding="utf-8")

    report, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report["files_reused"] == 2
    assert report["files_recomputed"] == 1

    after = {r["record_pk"]: r for r in _load_csv(records_dir / "records.csv")}
    changed_pks = [pk for pk in before if before[pk] != after.get(pk)]
    assert len(changed_pks) == 1
    assert "fp__proj__001__fingerprint.json" in changed_pks[0]
    # sig_hash changed (accuracy is in allowed_items) for the touched record only.
    assert before[changed_pks[0]]["sig_hash"] != after[changed_pks[0]]["sig_hash"]
    for pk in before:
        if pk not in changed_pks:
            assert before[pk] == after[pk]


def test_force_full_cache_matches_cold_cache_run(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 2)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    cold_root = tmp_path / "cold"
    report_cold, cold_dir = _run_incremental(tmp_path, exports, cold_root, sig_pol, join_pol)
    assert report_cold["files_recomputed"] == 2

    warm_root = tmp_path / "warm"
    _run_incremental(tmp_path, exports, warm_root, sig_pol, join_pol)
    report_forced, warm_dir = _run_incremental(tmp_path, exports, warm_root, sig_pol, join_pol, force_full=True)
    assert report_forced["files_recomputed"] == 2
    assert report_forced["files_reused"] == 0
    assert report_forced["cache_invalidation_reason"] == "force_full_requested"

    assert _load_csv(cold_dir / "records.csv") == _load_csv(warm_dir / "records.csv")


def test_sig_hash_policy_change_invalidates_whole_cache(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 2)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    results_root = tmp_path / "results"
    _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)

    mutated_policy = json.loads(sig_pol.read_text(encoding="utf-8"))
    mutated_policy["domains"]["units"]["notes"] = ["changed"]
    _write_json(sig_pol, mutated_policy)

    report, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report["cache_was_valid"] is False
    assert report["cache_invalidation_reason"] == "sig_hash_policy_changed"
    assert report["files_recomputed"] == 2
    assert report["files_reused"] == 0


def test_join_policy_change_invalidates_whole_cache(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 2)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    results_root = tmp_path / "results"
    _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)

    mutated_policy = json.loads(join_pol.read_text(encoding="utf-8"))
    mutated_policy["domains"]["units"]["optional_items"] = []
    _write_json(join_pol, mutated_policy)

    report, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report["cache_invalidation_reason"] == "join_policy_changed"
    assert report["files_recomputed"] == 2


def test_incremental_matches_non_incremental_pipeline_units_domain(tmp_path: Path) -> None:
    """Correctness proof: the new gate+per-file pass produces identical values
    to the existing always-full flatten -> sig_hash -> apply pipeline on the
    same corpus snapshot."""
    exports = tmp_path / "exports"
    _write_corpus(exports, 3)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    old_dir = tmp_path / "old"
    extractor.emit_records(exports, old_dir, file_id_mode="basename")
    items_csv = old_dir / "identity_items.csv"
    run_extract_all._append_line_pattern_synthetic_norm_hash(items_csv)
    old_diag = run_extract_all._apply_sig_hash_to_phase0(old_dir, sig_pol, None)
    run_extract_all._append_line_pattern_synthetic_norm_hash(items_csv)
    subprocess.run(
        [sys.executable, "tools/apply_join_policy.py", "--phase0-dir", str(old_dir), "--join-policy", str(join_pol)],
        check=True, cwd=str(Path(__file__).resolve().parent.parent),
    )

    new_report, new_dir = _run_incremental(tmp_path, exports, tmp_path / "new", sig_pol, join_pol)

    assert _load_csv(old_dir / "records.csv") == _load_csv(new_dir / "records.csv")
    assert _load_csv(old_dir / "file_metadata.csv") == _load_csv(new_dir / "file_metadata.csv")
    assert _load_csv(old_dir / "sig_basis_items.csv", exclude=()) == _load_csv(new_dir / "sig_basis_items.csv", exclude=())
    assert _load_csv(old_dir / "identity_items_by_domain" / "units.csv", exclude=()) == _load_csv(
        new_dir / "identity_items_by_domain" / "units.csv", exclude=()
    )

    new_diag = dict(new_report["sig_hash_diag"])
    assert dict(old_diag) == new_diag


def test_incremental_matches_non_incremental_pipeline_line_patterns_synthetic_hash(tmp_path: Path) -> None:
    """line_patterns is the highest-risk domain for this change: its join_key
    requires the synthetic line_pattern.segments_norm_hash item, normally
    appended by a separate corpus-wide shard rewrite between flatten and
    sig_hash. The incremental path folds that into the per-file pass instead
    -- this proves the two are equivalent."""
    sig_hash_policy = {
        "domains": {
            "line_patterns": {
                "allowed_item_prefixes": [],
                "allowed_items": ["line_pattern.segment_count", "line_pattern.segments_def_hash"],
                "hash_alg": "md5_utf8_join_pipe",
                "minima": {"block_if_any_required_not_ok": True},
                "required_items": ["line_pattern.segment_count", "line_pattern.segments_def_hash"],
                "sig_hash_schema": "line_patterns.sig_hash.v2",
            }
        }
    }
    join_policy = {
        "domains": {
            "line_patterns": {
                "join_key_schema": "line_patterns.join_key.v3",
                "hash_alg": "md5_utf8_join_pipe",
                "required_items": ["line_pattern.segments_norm_hash"],
                "optional_items": [],
                "explicitly_excluded_items": [
                    "line_pattern.uid", "line_pattern.name", "line_pattern.element_id",
                    "line_pattern.uid_or_namekey", "line_pattern.segments_def_hash",
                ],
            }
        }
    }

    def _lp_export(file_stem: str, seg_len: float) -> Dict:
        return {
            "_contract": {"domains": {"line_patterns": {"domain": "line_patterns", "status": "ok", "diag": {"count": 1}}}},
            "line_patterns": {
                "records": [
                    {
                        "record_id": f"{file_stem}-lp-1",
                        "status": "ok",
                        "identity_quality": "complete",
                        "sig_hash": "cafebabecafebabecafebabecafebabe",
                        "is_purgeable": False,
                        "instance_count": 1,
                        "label": {"display": "Dash", "quality": "ok", "provenance": "extractor", "components": {}},
                        "items": [
                            {"k": "line_pattern.segment_count", "v": "2", "q": "ok"},
                            {"k": "line_pattern.segments_def_hash", "v": "abc123", "q": "ok"},
                            {"k": "line_pattern.seg[000].kind", "v": "1", "q": "ok"},
                            {"k": "line_pattern.seg[000].length", "v": str(seg_len), "q": "ok"},
                            {"k": "line_pattern.seg[001].kind", "v": "2", "q": "ok"},
                            {"k": "line_pattern.seg[001].length", "v": "0", "q": "ok"},
                            {"k": "line_pattern.uid", "v": f"uid-{file_stem}", "q": "ok"},
                            {"k": "line_pattern.name", "v": "Dash", "q": "ok"},
                            {"k": "line_pattern.element_id", "v": "12345", "q": "ok"},
                            {"k": "line_pattern.uid_or_namekey", "v": f"uid-{file_stem}", "q": "ok"},
                        ],
                    }
                ]
            },
        }

    exports = tmp_path / "exports"
    exports.mkdir()
    for i in range(3):
        _write_json(exports / f"fp__lp__{i:03d}__fingerprint.json", _lp_export(f"file{i}", seg_len=1.0 + i))

    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, sig_hash_policy)
    _write_json(join_pol, join_policy)

    old_dir = tmp_path / "old"
    extractor.emit_records(exports, old_dir, file_id_mode="basename")
    items_csv = old_dir / "identity_items.csv"
    run_extract_all._append_line_pattern_synthetic_norm_hash(items_csv)
    old_diag = run_extract_all._apply_sig_hash_to_phase0(old_dir, sig_pol, None)
    subprocess.run(
        [sys.executable, "tools/apply_join_policy.py", "--phase0-dir", str(old_dir), "--join-policy", str(join_pol)],
        check=True, cwd=str(Path(__file__).resolve().parent.parent),
    )

    new_report, new_dir = _run_incremental(tmp_path, exports, tmp_path / "new", sig_pol, join_pol)

    old_records = _load_csv(old_dir / "records.csv")
    new_records = _load_csv(new_dir / "records.csv")
    assert old_records == new_records
    # Every record must have successfully resolved a join_hash -- proves the
    # synthetic segments_norm_hash item made it into the join-key computation.
    assert all(r["join_key_status"] == "ok" and r["join_hash"] for r in new_records)
    assert dict(old_diag) == dict(new_report["sig_hash_diag"])


def test_run_extract_all_cli_incremental_end_to_end(tmp_path: Path) -> None:
    """Full CLI wiring: --incremental through run_extract_all.py's main(),
    including the placeholders stage, which must always see the complete
    (cached + fresh) population regardless of cache state."""
    exports = tmp_path / "exports"
    _write_corpus(exports, 3)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    results_root = tmp_path / "cli_results"
    repo_root = Path(__file__).resolve().parent.parent
    cmd = [
        sys.executable, "tools/run_extract_all.py", str(exports),
        "--out-root", str(results_root), "--out-root-is-results-root",
        "--stages", "sig_hash,flatten,apply,placeholders",
        "--sig-hash-policy", str(sig_pol), "--join-policy", str(join_pol),
        "--incremental",
    ]
    r1 = subprocess.run(cmd, cwd=str(repo_root), check=True, capture_output=True, text=True)
    assert "files_recomputed=3" in r1.stdout

    before_records = _load_csv(results_root / "records" / "records.csv")
    placeholder_dir = results_root / "placeholder_exclusions"
    assert placeholder_dir.is_dir()
    before_placeholders = sorted(p.name for p in placeholder_dir.glob("*.csv"))
    before_ph_content = {p.name: p.read_bytes() for p in placeholder_dir.glob("*.csv")}

    r2 = subprocess.run(cmd, cwd=str(repo_root), check=True, capture_output=True, text=True)
    assert "files_reused=3" in r2.stdout
    assert "files_recomputed=0" in r2.stdout

    after_records = _load_csv(results_root / "records" / "records.csv")
    assert before_records == after_records
    after_placeholders = sorted(p.name for p in placeholder_dir.glob("*.csv"))
    assert before_placeholders == after_placeholders
    after_ph_content = {p.name: p.read_bytes() for p in placeholder_dir.glob("*.csv")}
    assert before_ph_content == after_ph_content


def test_split_file_signature_does_not_collapse_two_mtimes(tmp_path: Path) -> None:
    """Regression for a real bug: folding a split index/details pair's two
    (mtime_ns, size) stats together (XOR mtimes, sum sizes) can produce the
    same folded signature across two genuinely different content states --
    e.g. if the two files' mtimes simply swap between states, XOR is
    commutative and stays identical, and if sizes are unchanged (content
    edited without changing byte length) the sum stays identical too. That
    would wrongly skip the content-hash fallback and reuse stale rows. Each
    path's stat pair must be tracked and compared independently."""
    primary = tmp_path / "a.index.json"
    secondary = tmp_path / "a.details.json"
    primary.write_text('{"x": 1}', encoding="utf-8")   # 8 bytes
    secondary.write_text('{"y": 22}', encoding="utf-8")  # 9 bytes

    t1 = 1_700_000_000_000_000_000
    t2 = 1_700_000_100_000_000_000
    os.utime(primary, ns=(t1, t1))
    os.utime(secondary, ns=(t2, t2))

    cache_dir = tmp_path / "cache"
    file_id_to_paths = {"a.index.json": [primary, secondary]}
    state1 = run_a_cache.compute_run_state(
        cache_dir=cache_dir, file_id_to_paths=file_id_to_paths,
        sig_hash_policy_path=None, join_policy_path=None,
        tool_version="v1", force_full=False,
    )
    assert state1.cache_was_valid is False  # no prior manifest yet
    run_a_cache.save_entry(cache_dir, "a.index.json", {"marker": "first"})
    run_a_cache.finalize_manifest(state1)

    # Swap the two mtimes (same values, opposite files) and edit content while
    # holding each file's own byte length constant -- the naive XOR/sum fold
    # is identical to before; each file's own (mtime_ns, size) pair is not.
    primary.write_text('{"x": 9}', encoding="utf-8")  # still 8 bytes, different content
    os.utime(primary, ns=(t2, t2))
    os.utime(secondary, ns=(t1, t1))

    state2 = run_a_cache.compute_run_state(
        cache_dir=cache_dir, file_id_to_paths=file_id_to_paths,
        sig_hash_policy_path=None, join_policy_path=None,
        tool_version="v1", force_full=False,
    )
    assert "a.index.json" not in state2.unchanged_file_ids


def test_tool_version_change_invalidates_whole_cache(tmp_path: Path, monkeypatch) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 2)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)
    results_root = tmp_path / "results"

    monkeypatch.setenv("FINGERPRINT_TOOL_VERSION", "1.0.0")
    report1, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report1["files_recomputed"] == 2

    monkeypatch.setenv("FINGERPRINT_TOOL_VERSION", "1.0.1")
    report2, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report2["cache_invalidation_reason"] == "tool_version_changed"
    assert report2["files_recomputed"] == 2
    assert report2["files_reused"] == 0


_WIDGETS_SIG_HASH_POLICY_BLOCK = {
    "allowed_item_prefixes": [],
    "allowed_items": ["widget.color"],
    "hash_alg": "md5_utf8_join_pipe",
    "minima": {"block_if_any_required_not_ok": True},
    "required_items": ["widget.color"],
    "sig_hash_schema": "widgets.sig_hash.v1",
}


def _two_domain_export(file_stem: str) -> Dict:
    payload = _units_export(file_stem)
    payload["_contract"]["domains"]["widgets"] = {"domain": "widgets", "status": "ok", "diag": {"count": 1}}
    payload["widgets"] = {
        "records": [
            {
                "record_id": f"{file_stem}-widgets-1",
                "status": "ok",
                "identity_quality": "complete",
                "sig_hash": "bootstrapwidgethash0000000000001",
                "is_purgeable": False,
                "instance_count": 1,
                "label": {"display": "Widget", "quality": "ok", "provenance": "extractor", "components": {}},
                "items": [{"k": "widget.color", "v": "red", "q": "ok"}],
            }
        ]
    }
    return payload


def test_domain_filter_excludes_domain_from_sig_hash_even_with_a_policy(tmp_path: Path) -> None:
    """Regression: a domain outside the sig_hash filter must be left at
    flatten's bootstrap sig_hash, exactly like _apply_sig_hash_to_phase0's own
    dom_filter -- even when that domain DOES have a policy entry (this is not
    about "no policy exists", it's about "this domain wasn't in scope")."""
    exports = tmp_path / "exports"
    exports.mkdir()
    _write_json(exports / "fp__proj__000__fingerprint.json", _two_domain_export("file0"))

    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    combined_sig_policy = {
        "domains": {
            "units": _UNITS_SIG_HASH_POLICY["domains"]["units"],
            "widgets": _WIDGETS_SIG_HASH_POLICY_BLOCK,
        }
    }
    _write_json(sig_pol, combined_sig_policy)
    _write_json(join_pol, _UNITS_JOIN_POLICY)

    report, records_dir = _run_incremental(
        tmp_path, exports, tmp_path / "results", sig_pol, join_pol, sig_hash_domains=["units"],
    )
    rows = {r["domain"]: r for r in _load_csv(records_dir / "records.csv")}
    assert rows["units"]["sig_hash"] != "deadbeefdeadbeefdeadbeefdeadbeef"  # actually hashed
    assert rows["widgets"]["sig_hash"] == "bootstrapwidgethash0000000000001"  # untouched bootstrap
    assert "widgets" not in report["sig_hash_diag"]["domains_without_policy"]

    # Cross-check against the actual non-incremental pipeline with the same filter.
    old_dir = tmp_path / "old"
    extractor.emit_records(exports, old_dir, file_id_mode="basename")
    old_diag = run_extract_all._apply_sig_hash_to_phase0(old_dir, sig_pol, ["units"])
    old_rows = {r["domain"]: r for r in _load_csv(old_dir / "records.csv")}
    assert old_rows["units"]["sig_hash"] == rows["units"]["sig_hash"]
    assert old_rows["widgets"]["sig_hash"] == rows["widgets"]["sig_hash"]
    assert "widgets" not in old_diag["domains_without_policy"]


def test_domain_filter_change_invalidates_whole_cache(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    exports.mkdir()
    _write_json(exports / "fp__proj__000__fingerprint.json", _two_domain_export("file0"))
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, {"domains": {
        "units": _UNITS_SIG_HASH_POLICY["domains"]["units"],
        "widgets": _WIDGETS_SIG_HASH_POLICY_BLOCK,
    }})
    _write_json(join_pol, _UNITS_JOIN_POLICY)
    results_root = tmp_path / "results"

    report1, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol, sig_hash_domains=["units"])
    assert report1["files_recomputed"] == 1

    report2, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol, sig_hash_domains=["units", "widgets"])
    assert report2["cache_invalidation_reason"] == "domain_filter_changed"
    assert report2["files_recomputed"] == 1
    assert report2["files_reused"] == 0


def test_line_patterns_record_with_no_items_hard_fails(tmp_path: Path) -> None:
    """A line_patterns record with zero identity items can never get the
    synthetic segments_norm_hash item -- must hard-fail loudly (matching
    run_extract_all._validate_line_pattern_synthetic_norm_hash /
    apply_join_policy.py's own PK check) instead of silently caching a
    blocked/missing-join_hash result."""
    exports = tmp_path / "exports"
    exports.mkdir()
    payload = {
        "_contract": {"domains": {"line_patterns": {"domain": "line_patterns", "status": "ok", "diag": {"count": 1}}}},
        "line_patterns": {
            "records": [
                {
                    "record_id": "file0-lp-1",
                    "status": "ok",
                    "identity_quality": "complete",
                    "sig_hash": "cafebabecafebabecafebabecafebabe",
                    "is_purgeable": False,
                    "instance_count": 1,
                    "label": {"display": "Dash", "quality": "ok", "provenance": "extractor", "components": {}},
                    "items": [],  # no items at all -> no synthetic item possible
                }
            ]
        },
    }
    _write_json(exports / "fp__lp__000__fingerprint.json", payload)

    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, {"domains": {}})
    _write_json(join_pol, {"domains": {}})

    with pytest.raises(SystemExit, match="segments_norm_hash"):
        _run_incremental(tmp_path, exports, tmp_path / "results", sig_pol, join_pol)


def test_malformed_join_policy_hard_fails_like_apply_join_policy(tmp_path: Path) -> None:
    """apply_join_policy.py raises SystemExit("Invalid policy format: missing
    domains") on a malformed policy file rather than silently treating every
    domain as policy-less. The incremental path must match, not quietly cache
    a run where every record is join_key_status=missing_policy."""
    exports = tmp_path / "exports"
    _write_corpus(exports, 1)
    sig_pol = tmp_path / "sig_hash_policy.json"
    join_pol = tmp_path / "join_policy.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, {"not_domains": {}})  # malformed: no "domains" key

    with pytest.raises(SystemExit, match="Invalid policy format"):
        _run_incremental(tmp_path, exports, tmp_path / "results", sig_pol, join_pol)


def test_warm_cache_deserializes_each_candidate_once(tmp_path: Path, monkeypatch) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 3)
    sig_pol = tmp_path / "sig.json"; join_pol = tmp_path / "join.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY); _write_json(join_pol, _UNITS_JOIN_POLICY)
    results_root = tmp_path / "results"
    _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    calls = 0
    original = run_a_cache.load_entry_diagnostic
    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)
    monkeypatch.setattr(run_a_incremental.run_a_cache, "load_entry_diagnostic", counted)
    report, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert calls == report["files_total"] == 3
    assert report["performance"]["cache_entry_loads"] == 3
    assert report["files_reused"] == 3
    assert report["performance"]["source_files_hashed"] == 0


def test_non_utf8_cache_entry_is_unreadable_fallback(tmp_path: Path) -> None:
    exports = tmp_path / "exports"
    _write_corpus(exports, 1)
    sig_pol = tmp_path / "sig.json"
    join_pol = tmp_path / "join.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, _UNITS_JOIN_POLICY)
    results_root = tmp_path / "results"
    _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)

    cache_dir = run_a_cache.cache_root(results_root)
    entry_path = next((cache_dir / "entries").glob("*.json"))
    entry_path.write_bytes(b"\xff\xfe\x80")

    report, _ = _run_incremental(tmp_path, exports, results_root, sig_pol, join_pol)
    assert report["files_reused"] == 0
    assert report["files_recomputed"] == 1
    assert report["performance"]["fallback_reasons"] == {"unreadable": 1}


def test_failed_run_stops_heartbeat(tmp_path: Path) -> None:
    import io
    from tools.progress_reporter import ProgressReporter

    exports = tmp_path / "exports"
    _write_corpus(exports, 1)
    sig_pol = tmp_path / "sig.json"
    join_pol = tmp_path / "join.json"
    _write_json(sig_pol, _UNITS_SIG_HASH_POLICY)
    _write_json(join_pol, {"not_domains": {}})
    reporter = ProgressReporter(interval=60, stream=io.StringIO())

    with pytest.raises(SystemExit, match="Invalid policy format"):
        run_a_incremental.run_incremental(
            exports,
            tmp_path / "results" / "records",
            cache_dir=tmp_path / "results" / ".run_a_cache",
            sig_hash_policy_path=sig_pol,
            join_policy_path=join_pol,
            progress=reporter,
        )

    assert reporter._thread is None
    assert reporter._stop.is_set()
