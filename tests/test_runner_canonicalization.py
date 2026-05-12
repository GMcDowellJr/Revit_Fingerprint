from core.canonical_items import canonicalize_record


def test_canonicalize_record_merges_all_sources_and_strips_legacy_keys():
    rec = {
        "domain": "identity",
        "record_id": "R1",
        "status": "ok",
        "status_reasons": [],
        "label": {"display": "x"},
        "items": [{"k": "a", "v": "1", "q": "ok", "role": "unknown"}],
        "identity_basis": {"items": [{"k": "b", "v": "2", "q": "ok"}]},
        "phase2": {
            "semantic_items": [{"k": "c", "v": "3", "q": "ok"}],
            "lineage_items": [{"k": "d", "v": "4", "q": "ok"}],
            "cosmetic_items": [{"k": "e", "v": "5", "q": "ok"}],
            "coordination_items": [{"k": "f", "v": "6", "q": "ok"}],
            "unknown_items": [{"k": "g", "v": "7", "q": "ok"}],
        },
        "sig_hash": "abc",
        "join_key": {},
    }

    out = canonicalize_record(rec)
    assert [it["k"] for it in out["items"]] == ["a", "b", "c", "d", "e", "f", "g"]
    assert all(set(it.keys()) == {"k", "v", "q"} for it in out["items"])
    for k in ("identity_basis", "phase2", "join_key", "sig_hash", "sig_basis", "identity_quality", "record_id_alg", "record_id_scope", "schema_version"):
        assert k not in out
