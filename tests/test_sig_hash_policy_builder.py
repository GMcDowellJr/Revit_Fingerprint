import json
import os

from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK, make_identity_item, serialize_identity_items
from core.hashing import make_hash
from core.sig_hash_builder import build_sig_hash_from_policy, apply_sig_hash_policy_to_record
from core.sig_hash_policy import load_sig_hash_policies, get_domain_sig_hash_policy


def test_generated_sig_hash_policy_loads():
    policies = load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json"))
    assert policies["version"] == "domain_sig_hash_policies.v1"
    assert get_domain_sig_hash_policy(policies, "line_patterns") is not None


def test_sig_hash_builder_hashes_allowed_items_order_independent():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["b", "a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [make_identity_item("b", "2", ITEM_Q_OK), make_identity_item("a", "1", ITEM_Q_OK)]
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, identity_items=items)
    assert status == "ok"
    assert reasons == []
    assert sig_hash == make_hash(serialize_identity_items(items))


def test_sig_hash_builder_blocks_when_required_not_ok():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    sig_hash, status, reasons, _ = build_sig_hash_from_policy(
        domain_policy=policy,
        identity_items=[make_identity_item("a", None, ITEM_Q_MISSING)],
    )
    assert sig_hash is None
    assert status == "blocked"
    assert "identity.incomplete:required_not_ok:a" in reasons


def test_apply_sig_hash_policy_to_record_updates_record():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    record = {
        "schema_version": "record.v2",
        "status": "ok",
        "status_reasons": [],
        "sig_hash": "placeholder",
        "identity_basis": {"hash_alg": "md5_utf8_join_pipe", "item_schema": "identity_items.v1", "items": [make_identity_item("a", "1", ITEM_Q_OK)]},
        "identity_quality": "complete",
    }
    out = apply_sig_hash_policy_to_record(record, policy)
    assert out["sig_hash"] == make_hash(serialize_identity_items(record["identity_basis"]["items"]))
    assert out["identity_basis"]["sig_hash_schema"] == "x.sig_hash.v1"
