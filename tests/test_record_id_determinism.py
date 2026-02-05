# tests/test_record_id_determinism.py
from core.record_v2 import (
    STATUS_BLOCKED,
    STATUS_OK,
    finalize_record_ids_for_domain,
    make_record_id_structural,
)
from validators.record_v2 import validate_records_v2


def _make_record(base_id, alg, sort_key, label_display):
    return {
        "record_id": base_id,
        "record_id_alg": alg,
        "record_id_base": base_id,
        "record_id_sort_key": sort_key,
        "status": STATUS_OK,
        "status_reasons": [],
        "sig_hash": "deadbeefdeadbeefdeadbeefdeadbeef",
        "identity_items": [],
        "label": {"display": label_display},
    }


def test_structural_record_id_dup_index_deterministic():
    base_id, alg, _canon = make_record_id_structural({"name": "example"})
    records = [
        _make_record(base_id, alg, "b", "Label B"),
        _make_record(base_id, alg, "a", "Label A"),
    ]

    finalize_record_ids_for_domain(records)

    by_key = {rec["record_id_sort_key"]: rec["record_id"] for rec in records}
    assert by_key["a"] == f"{base_id}:000"
    assert by_key["b"] == f"{base_id}:001"


def test_structural_record_id_duplicate_keys_blocked():
    base_id, alg, _canon = make_record_id_structural({"name": "duplicate"})
    records = [
        _make_record(base_id, alg, "same", "Label"),
        _make_record(base_id, alg, "same", "Label"),
    ]

    finalize_record_ids_for_domain(records)

    for rec in records:
        assert rec["status"] == STATUS_BLOCKED
        assert "unstable_record_id_no_structural_key" in rec["status_reasons"]
        assert rec["sig_hash"] is None


def test_structural_record_id_stable_across_runs():
    def _build_records_in_order(order):
        base_id, alg, _canon = make_record_id_structural({"domain": "vf", "shape": "same"})
        return [
            _make_record(base_id, alg, f"sort:{idx}", f"Label {idx}")
            for idx in order
        ]

    run_a = _build_records_in_order([2, 0, 1])
    run_b = _build_records_in_order([1, 2, 0])

    finalize_record_ids_for_domain(run_a)
    finalize_record_ids_for_domain(run_b)

    ids_a = {r["record_id"] for r in run_a}
    ids_b = {r["record_id"] for r in run_b}
    assert ids_a == ids_b


def test_validate_records_duplicate_within_file_and_domain():
    registry = {
        "record_schema_version": "record.v2",
        "identity_item_schema": "identity_items.v1",
        "banned_identity_value_substrings": [],
        "identity_quality": {"dominance_order": ["incomplete_unreadable", "incomplete_unsupported", "incomplete_missing", "complete"]},
        "domains": {
            "demo": {
                "allowed_keys": ["k1"],
                "allowed_key_prefixes": [],
                "indexed_key_rules": {},
                "required_keys": ["k1"],
                "minima": {"block_if_any_required_not_ok": True},
            }
        },
    }
    rec = {
        "schema_version": "record.v2",
        "domain": "demo",
        "record_id": "uid:abc",
        "record_id_alg": "revit_uniqueid_v1",
        "record_id_scope": "file_local",
        "status": "ok",
        "status_reasons": [],
        "sig_hash": "deadbeefdeadbeefdeadbeefdeadbeef",
        "identity_basis": {
            "hash_alg": "md5_utf8_join_pipe",
            "item_schema": "identity_items.v1",
            "items": [{"k": "k1", "q": "ok", "v": "v1"}],
        },
        "identity_quality": "complete",
        "label": {"display": "Demo", "quality": "human", "provenance": "none", "components": {}},
        "file_id": "file-A",
    }

    violations = validate_records_v2([rec, dict(rec)], registry)
    assert any(v.endswith("record_id.duplicate:file-A:demo") for _, v in violations)
