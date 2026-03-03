import hashlib
import json

from core.export_payload import build_export_payload


def _legacy_sample(*, include_missing_domain=False):
    domains = {
        "line_patterns": {
            "block_reasons": ["none"],
        }
    }
    payload = {
        "_contract": {"domains": domains},
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
                    "join_key": {"join_hash": "md5:def", "keys_used": ["alpha"]},
                    "sig_basis": {"keys_used": ["alpha"]},
                    "phase2": {
                        "unknown_items": [
                            {"k": "line_pattern.source_unique_id", "v": "uid-1", "q": "ok"},
                            {"k": "line_pattern.source_element_id", "v": "42", "q": "ok"},
                            {"k": "line_pattern.display.label", "v": "LP", "q": "ok"},
                            {"k": "line_pattern.some_unknown", "v": "z", "q": "unsupported"},
                        ]
                    },
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
                },
            ],
        },
    }
    if include_missing_domain:
        payload["_contract"]["domains"]["missing_domain"] = {"block_reasons": []}
        payload["missing_domain"] = {"count": 0, "records": []}
    return payload


def _write_policy(tmp_path):
    policy = {
        "schema_version": "0.2.3",
        "domains": {
            "line_patterns": {
                "join_key_schema": "line_patterns.join_key.v1",
                "hash_alg": "md5_utf8_join_pipe",
                "required_items": ["alpha", "beta"],
                "optional_items": ["gamma"],
                "explicitly_excluded_items": ["line_pattern.uid"],
                "shape_gating": {"kind": "none"},
            }
        },
    }
    path = tmp_path / "domain_join_key_policies.json"
    raw = json.dumps(policy, sort_keys=True).encode("utf-8")
    path.write_bytes(raw)
    return path, raw


def test_policy_ref_hash_and_domain_policies_from_registry(tmp_path):
    policy_path, raw = _write_policy(tmp_path)
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
        policy_registry_path=str(policy_path),
    )
    pref = out["manifest"]["policy_ref"]
    assert len(pref["policy_hash"]) == 64
    assert pref["policy_hash"] == hashlib.sha256(raw).hexdigest()
    assert pref["policy_version"] == "0.2.3"
    assert pref["source"] == "external"

    policies = out["manifest"]["domain_policies"]
    assert "domains" not in policies
    assert list(policies.keys()) == ["line_patterns"]
    assert policies["line_patterns"]["required_keys"] == ["alpha", "beta"]
    assert policies["line_patterns"]["optional_keys"] == ["gamma"]
    assert policies["line_patterns"]["explicitly_excluded_keys"] == ["line_pattern.uid"]


def test_missing_policy_sets_flag_and_domain_warning(tmp_path):
    policy_path, _ = _write_policy(tmp_path)
    out = build_export_payload(
        legacy_payload=_legacy_sample(include_missing_domain=True),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
        policy_registry_path=str(policy_path),
    )
    assert out["manifest"]["domain_policies"]["missing_domain"] == {"missing_policy": True}
    assert "missing_policy" in out["domains"]["missing_domain"]["diag"]["warnings"]


def test_blocked_reason_normalization_and_summary_counts(tmp_path):
    policy_path, _ = _write_policy(tmp_path)
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
        policy_registry_path=str(policy_path),
    )
    dom = out["domains"]["line_patterns"]
    assert dom["summary"]["exported_count"] == len(dom["records"]) == 1
    assert dom["summary"]["blocked_count"] == 1
    b = dom["diag"]["blocked_records"][0]
    assert set(b.keys()) == {"label", "class", "detail", "reasons"}
    assert b["class"] in {"not_applicable", "unavailable", "policy_omitted", "error"}


def test_unclassified_cleanup_no_unknown_items_and_no_definition_duplicates(tmp_path):
    policy_path, _ = _write_policy(tmp_path)
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version="0.0.0+abc",
        tool_git_sha="abc",
        host_app_version="2025",
        policy_registry_path=str(policy_path),
    )
    rec = out["domains"]["line_patterns"]["records"][0]
    assert "unknown_items" not in str(rec)
    def_keys = {it["k"] for it in rec["definition"]["items"]}
    for it in rec.get("diagnostics", {}).get("unclassified_items", []):
        assert it["k"] not in def_keys
    assert rec["provenance"]["source"]["element_unique_id"] == "uid-1"
    assert rec["provenance"]["source"]["element_id"] == 42


def test_meta_fallback_placeholders_when_metadata_missing(tmp_path):
    policy_path, _ = _write_policy(tmp_path)
    out = build_export_payload(
        legacy_payload=_legacy_sample(),
        tool_version=None,
        tool_git_sha=None,
        host_app_version=None,
        policy_registry_path=str(policy_path),
    )
    exporter = out["meta"]["tools"]["exporter"]
    assert exporter["name"] == "revit_fingerprint"
    assert exporter["version"] == "0.0.0+unknown"
    assert exporter["git_sha"] == "unknown"
    assert out["meta"]["host"]["python"].startswith("CPython ")
