from core.export_payload import build_export_payload


def _legacy_sample():
    return {
        "_contract": {
            "domains": {
                "line_patterns": {
                    "block_reasons": ["none"],
                }
            }
        },
        "_join_key_policies": {
            "line_patterns": {
                "policy_id": "line_patterns.join_key",
                "version": "0.1.0",
                "required": ["a"],
                "optional": ["b"],
            }
        },
        "_notes": ["note"],
        "line_patterns": {
            "count": 1,
            "records": [
                {
                    "domain": "line_patterns",
                    "record_id": "r1",
                    "status": "ok",
                    "status_reasons": [],
                    "sig_hash": "md5:abc",
                    "label": {"display": "LP"},
                    "identity_basis": {
                        "items": [
                            {"k": "alpha", "t": "s", "v": "x", "q": "ok"},
                            {"k": "beta", "v": 1, "q": "missing"},
                        ]
                    },
                    "join_key": {"join_hash": "md5:def", "keys_used": ["alpha"], "selectors": ["alpha"], "schema": "jk.v1"},
                    "sig_basis": {"keys_used": ["alpha"]},
                    "phase2": {
                        "unknown_items": [
                            {"k": "line_pattern.source_unique_id", "v": "uid-1", "q": "ok"},
                            {"k": "line_pattern.source_element_id", "v": "42", "q": "ok"},
                            {"k": "line_pattern.some_unknown", "v": "z", "q": "unsupported"},
                        ]
                    },
                    "features": {"items": [{"k": "dup", "v": "dup"}]},
                    "identity_basis_extra": {"items": []},
                }
            ],
        },
    }


def test_export_payload_shape_and_required_paths():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    assert set(out.keys()) == {"contract", "manifest", "meta", "notes", "domains"}
    rec = out["domains"]["line_patterns"]["records"][0]
    assert rec["domain"] == "line_patterns"
    assert rec["id"]["sig_hash"] == "md5:abc"
    assert rec["id"]["join_hash"] == "md5:def"
    assert rec["label"]["display"] == "LP"
    assert "definition" in rec and isinstance(rec["definition"]["items"], list)


def test_legacy_keys_removed_and_t_enum_q_enforced():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    rec = out["domains"]["line_patterns"]["records"][0]
    assert "phase2" not in rec
    assert "features" not in rec
    assert "identity_basis" not in rec
    items = rec["definition"]["items"]
    assert {it["t"] for it in items}.issubset({"s", "i", "f", "b", "json"})
    assert {it["q"] for it in items}.issubset({"ok", "warn", "unknown"})
    assert rec["provenance"]["source"]["element_unique_id"] == "uid-1"
    assert rec["provenance"]["source"]["element_id"] == 42
    assert rec["diagnostics"]["unknown_items"][0]["k"] == "line_pattern.some_unknown"
