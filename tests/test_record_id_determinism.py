# tests/test_record_id_determinism.py
from core.record_v2 import (
    STATUS_BLOCKED,
    STATUS_OK,
    finalize_record_ids_for_domain,
    make_record_id_structural,
)


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
