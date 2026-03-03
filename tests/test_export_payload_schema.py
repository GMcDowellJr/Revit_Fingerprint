from core.export_payload import build_export_payload


def _legacy_sample(*, nested_policy_shape=False):
    domain_policies = {
        "line_patterns": {
            "policy_id": "line_patterns.join_key",
            "version": "0.1.0",
            "required": ["a"],
            "optional": ["b"],
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


def test_summary_counts_and_non_null_meta_fields_with_thinrunner_precedence():
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version=None,
        tool_git_sha=None,
        host_app_version=None,
        thinrunner_meta={
            "exporter": {"name": "thinrunner_fp", "version": "0.9.0", "git_sha": "deadbee"},
            "host": {"python": "CPython 3.9.12", "app": "Revit", "app_version": "2024"},
        },
    )
    dom = out["domains"]["line_patterns"]
    assert dom["summary"]["exported_count"] == len(dom["records"])
    assert dom["summary"]["blocked_count"] == 0
    assert dom["summary"]["raw_count"] == 1

    exporter = out["meta"]["tools"]["exporter"]
    assert exporter["name"] == "thinrunner_fp"
    assert exporter["version"] == "0.9.0"
    assert exporter["git_sha"] == "deadbee"
    assert out["meta"]["host"]["python"] == "CPython 3.9.12"


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
