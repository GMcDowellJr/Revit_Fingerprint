import os

from core.record_v2 import ITEM_Q_MISSING, ITEM_Q_OK, make_identity_item, serialize_identity_items
from core.hashing import make_hash
from core.sig_hash_builder import build_sig_hash_from_policy, apply_sig_hash_policy_to_record
from core.sig_hash_policy import load_sig_hash_policies, get_domain_sig_hash_policy


def test_generated_sig_hash_policy_loads():
    policies = load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json"))
    assert policies["version"] == "domain_sig_hash_policies.v1"
    assert get_domain_sig_hash_policy(policies, "line_patterns") is not None


def test_sig_hash_builder_hashes_allowed_items_from_items_list_order_independent():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["b", "a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [make_identity_item("b", "2", ITEM_Q_OK), make_identity_item("a", "1", ITEM_Q_OK)]
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    assert reasons == []
    assert sig_hash == make_hash(serialize_identity_items(items))
    assert [it["k"] for it in hash_items] == ["b", "a"]


def test_sig_hash_builder_blocks_when_required_not_ok():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    item = make_identity_item("a", None, ITEM_Q_MISSING)
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
        domain_policy=policy,
        items=[item],
    )
    # Blocked records still produce a hash for traceability (based on available items).
    assert sig_hash == make_hash(serialize_identity_items(hash_items))
    assert status == "blocked"
    assert "identity.incomplete:required_not_ok:a" in reasons


def test_sig_hash_builder_degrades_when_required_not_ok_and_block_disabled():
    policy = {
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": False},
    }
    sig_hash, status, reasons, _ = build_sig_hash_from_policy(domain_policy=policy, items=[{"k": "a", "v": None, "q": "missing"}])
    assert status == "degraded"
    assert sig_hash is not None
    assert "identity.incomplete:required_not_ok:a" in reasons


def test_sig_hash_builder_prefix_and_first_writer_wins():
    policy = {
        "allowed_items": [],
        "allowed_item_prefixes": ["x."],
        "required_items": ["x.a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    items = [
        {"k": "x.a", "v": "1", "q": "ok"},
        {"k": "x.a", "v": "2", "q": "missing"},
        {"k": "x.b", "v": "3", "q": "ok"},
    ]
    sig_hash, status, _, hash_items = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    assert sig_hash is not None
    assert [it["k"] for it in hash_items] == ["x.a", "x.a", "x.b"]


def test_apply_sig_hash_policy_to_record_uses_items_and_writes_sig_basis():
    policy = {
        "sig_hash_schema": "x.sig_hash.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "allowed_items": ["a"],
        "allowed_item_prefixes": [],
        "required_items": ["a"],
        "minima": {"block_if_any_required_not_ok": True},
    }
    record = {
        "status": "ok",
        "status_reasons": [],
        "items": [{"k": "a", "v": "1", "q": "ok"}],
    }
    out = apply_sig_hash_policy_to_record(record, policy)
    assert out["sig_hash"] == make_hash(serialize_identity_items(record["items"]))
    assert out["sig_basis"]["schema"] == "x.sig_hash.v1"
    assert out["sig_basis"]["keys_used"] == ["a"]
    assert "identity_basis" not in out
    assert "identity_quality" not in out


def test_text_types_regression_hash_matches_legacy_preimage_semantics():
    policy = get_domain_sig_hash_policy(load_sig_hash_policies(os.path.join("policies", "domain_sig_hash_policies.json")), "text_types")
    items = [
        {"k": "text_type.name", "v": "Notes-Medium", "q": "ok"},
        {"k": "text_type.font", "v": "Arial", "q": "ok"},
        {"k": "text_type.size_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.width_factor", "v": "1.000000", "q": "ok"},
        {"k": "text_type.background", "v": "1", "q": "ok"},
        {"k": "text_type.line_weight", "v": "1", "q": "ok"},
        {"k": "text_type.color_rgb", "v": "0-0-0", "q": "ok"},
        {"k": "text_type.show_border", "v": "false", "q": "ok"},
        {"k": "text_type.leader_border_offset_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.tab_size_in", "v": "1.000000", "q": "ok"},
        {"k": "text_type.bold", "v": "false", "q": "ok"},
        {"k": "text_type.italic", "v": "false", "q": "ok"},
        {"k": "text_type.underline", "v": "false", "q": "ok"},
    ]
    sig_hash, status, _, _ = build_sig_hash_from_policy(domain_policy=policy, items=items)
    assert status == "ok"
    expected = make_hash(serialize_identity_items(items))
    assert sig_hash == expected
