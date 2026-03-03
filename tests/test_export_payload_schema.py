from core.export_payload import build_export_payload


def _legacy_sample(*, nested_policy_shape=False):
    domain_policies = {
        "line_patterns": {
            "policy_id": "line_patterns.join_key",
            "version": "0.1.0",
            "required": ["a"],
            "optional": ["b"],
            "signature_keys": ["a", "sig_only"],
        }
    }
    if nested_policy_shape:
        domain_policies = {"domains": domain_policies}

    return {
        "_contract": {
            "domains": {
                "line_patterns": {
                    "block_reasons": ["none"],
                }
            }
        },
        "_join_key_policies": domain_policies,
        "_notes": ["note"],
        "line_patterns": {
            "count": 2,
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
                            {"k": "line_pattern.display.label", "v": "LP label", "q": "ok"},
                            {"k": "line_pattern.some_unknown", "v": "z", "q": "unsupported"},
                        ]
                    },
                    "features": {"items": [{"k": "dup", "v": "dup"}]},
                },
                {
                    "domain": "line_patterns",
                    "record_id": "r2",
                    "status": "blocked",
                    "status_reasons": ["blocked_upstream"],
                    "sig_hash": None,
                    "label": {"display": "BLOCKED"},
                    "identity_basis": {"items": [{"k": "gamma", "v": "y", "q": "ok"}]},
                    "join_key": {"join_hash": None, "keys_used": ["gamma"]},
                    "phase2": {"unknown_items": [{"k": "line_pattern.uid", "v": "uid-2", "q": "ok"}]},
                },
            ],
        },
    }


def test_export_payload_shape_and_required_paths_and_block_filtering():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    assert set(out.keys()) == {"contract", "manifest", "meta", "notes", "domains"}

    dom = out["domains"]["line_patterns"]
    assert dom["summary"]["exported_count"] == len(dom["records"]) == 1
    assert dom["summary"]["blocked_count"] == 1
    assert "blocked_records" in dom["diag"]

    rec = dom["records"][0]
    assert rec["domain"] == "line_patterns"
    assert rec["id"]["sig_hash"] == "md5:abc"
    assert rec["id"]["join_hash"] == "md5:def"
    assert rec["label"]["display"] == "LP"
    assert "definition" in rec and isinstance(rec["definition"]["items"], list)


def test_no_unknown_items_and_intended_use_and_dedupe():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    rec = out["domains"]["line_patterns"]["records"][0]
    assert "unknown_items" not in str(rec)
    assert "phase2" not in rec
    assert "features" not in rec
    assert "identity_basis" not in rec

    items = rec["definition"]["items"]
    assert items and all(it.get("u") == "def" for it in items)
    assert {it["t"] for it in items}.issubset({"s", "i", "f", "b", "json"})
    assert {it["q"] for it in items}.issubset({"ok", "warn", "unknown"})

    def_keys = {it["k"] for it in items}
    for pit in rec["provenance"]["source"].keys():
        assert pit not in def_keys

    assert rec["provenance"]["source"]["element_unique_id"] == "uid-1"
    assert rec["provenance"]["source"]["element_id"] == 42
    assert rec["label"]["meta"]["line_pattern.display.label"] == "LP label"
    assert rec["diagnostics"]["unclassified_items"][0]["k"] == "line_pattern.some_unknown"


def test_domain_policies_is_domain_mapping_and_supports_legacy_nested_shape():
    out = build_export_payload(
        legacy_payload=_legacy_sample(nested_policy_shape=True),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    policies = out["manifest"]["domain_policies"]
    assert "domains" not in policies
    assert list(policies.keys()) == ["line_patterns"]
    assert policies["line_patterns"]["required_keys"] == ["a"]
    assert policies["line_patterns"]["signature_keys"] == ["a", "sig_only"]


def test_audit_mode_includes_basis_and_blocked_audit_records(monkeypatch):
    monkeypatch.setenv("REVIT_FINGERPRINT_EXPORT_MODE", "audit")
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
    )
    rec = out["domains"]["line_patterns"]["records"][0]
    assert "audit" in rec
    assert sorted(rec["audit"].keys()) == ["join_basis", "sig_basis"]
    assert "audit_blocked_records" in out["domains"]["line_patterns"]["diag"]


def test_meta_fallback_placeholders_when_metadata_missing():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version=None,
        tool_git_sha=None,
        host_app_version=None,
    )
    exporter = out["meta"]["tools"]["exporter"]
    assert exporter["name"] == "revit_fingerprint"
    assert exporter["version"] == "0.0.0+unknown"
    assert exporter["git_sha"] == "unknown"
    assert out["meta"]["host"]["python"].startswith("CPython ")
