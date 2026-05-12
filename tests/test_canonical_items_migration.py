from core.canonical_items import (
    build_flat_items,
    compile_role_policy,
    merge_legacy_buckets,
    resolve_item_roles,
)


def test_merge_legacy_buckets_to_flat_items_equivalence_and_dedupe():
    legacy = {
        "semantic_items": [{"k": "a", "v": "1", "q": "ok"}, {"k": "b", "v": "2", "q": "ok"}],
        "cosmetic_items": [{"k": "b", "v": "OVERRIDE", "q": "ok"}, {"k": "c", "v": "3", "q": "missing"}],
        "coordination_items": [{"k": "d", "v": "4", "q": "ok"}],
        "unknown_items": [{"k": "e", "v": None, "q": "unreadable"}],
    }

    out = merge_legacy_buckets(legacy)
    assert [it["k"] for it in out["items"]] == ["a", "b", "c", "d", "e"]
    assert [it for it in out["items"] if it["k"] == "b"][0]["v"] == "2"


def test_build_flat_items_preserves_counts_for_unique_keys():
    a = [{"k": "x", "v": "1", "q": "ok"}]
    b = [{"k": "y", "v": "2", "q": "ok"}]
    out = build_flat_items(a, b)
    assert len(out) == len(a) + len(b)


def test_compile_and_resolve_roles_runtime_from_key_only():
    policy = {
        "text_types": {
            "identity": ["text_type.leader_arrowhead_sig_hash"],
            "coordination": ["text_type.name", "text_type.type_id"],
        }
    }
    lookup = compile_role_policy(policy, domain="text_types")
    assert lookup["text_type.name"] == "coordination"

    items = [
        {"k": "text_type.name", "v": "Body", "q": "ok"},
        {"k": "text_type.leader_arrowhead_sig_hash", "v": "abc", "q": "ok"},
        {"k": "text_type.unmapped", "v": "x", "q": "ok"},
    ]
    grouped = resolve_item_roles(items, lookup)

    assert len(grouped["coordination"]) == 1
    assert len(grouped["identity"]) == 1
    assert len(grouped["unknown"]) == 1
    assert all("role" not in it for role in grouped.values() for it in role)
