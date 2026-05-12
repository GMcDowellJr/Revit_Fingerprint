from tools.migration.reformat_to_flat_items import transform_record


def test_identity_lineage_items_are_preserved_in_canonical_conversion():
    legacy = {
        "domain": "identity",
        "record_id": "r1",
        "status": "ok",
        "status_reasons": [],
        "label": {"display": "Identity"},
        "identity_basis": {
            "items": [
                {"k": "identity.project_title", "v": "Acme", "q": "ok"},
            ]
        },
        "phase2": {
            "lineage_items": [
                {"k": "identity.central_path", "v": "C:/proj/model.rvt", "q": "ok"},
                {"k": "identity.central_path_norm", "v": "c:/proj/model.rvt", "q": "ok"},
                {"k": "identity.filename", "v": "model.rvt", "q": "ok"},
            ],
            "cosmetic_items": [{"k": "identity.client_label", "v": "ClientA", "q": "ok", "role": "cosmetic"}],
            "unknown_items": [{"k": "identity.extra", "v": "x", "q": "ok", "role": "unknown"}],
        },
        "join_key": {"x": 1},
        "sig_hash": "abc",
        "sig_basis": {"y": 2},
    }

    out, *_ = transform_record(legacy, "identity")
    kqv = {(it["k"], it.get("v"), it.get("q")) for it in out["items"]}

    assert ("identity.central_path", "C:/proj/model.rvt", "ok") in kqv
    assert ("identity.central_path_norm", "c:/proj/model.rvt", "ok") in kqv
    assert ("identity.filename", "model.rvt", "ok") in kqv

    assert all("role" not in item for item in out["items"])
    assert "identity_basis" not in out
    assert "phase2" not in out
    assert "join_key" not in out
    assert "sig_hash" not in out
    assert "sig_basis" not in out
