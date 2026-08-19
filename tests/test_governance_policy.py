"""Tests for tools/governance_policy.py's generic JSON policy-profile loader.

Pure tests against the loader mechanics -- no dependency on
generate_governance_narrative.py's own default profile content (that
round-trip is covered separately in
test_generate_governance_narrative_policy.py). See
docs/governance_evidence_package.md.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from governance_policy import (  # noqa: E402
    ANOMALY_THRESHOLDS_FILENAME,
    CLIENT_ONBOARDING_FILENAME,
    DEFAULT_POLICY_DIR,
    DOMAIN_POLICY_FILENAME,
    FINDING_RULES_FILENAME,
    THRESHOLDS_FILENAME,
    load_governance_policy,
)

_DEFAULTS = {
    "thresholds": {"profile_id": "default-thresholds", "thresholds": {"x": 1}},
    "domain_policy": {"profile_id": "default-domain-policy"},
    "client_onboarding": {"profile_id": "default-client-onboarding"},
    "finding_rules": {"profile_id": "default-finding-rules"},
    "anomaly_thresholds": {"profile_id": "default-anomaly-thresholds"},
}


def test_default_policy_dir_is_repo_policies_governance():
    assert DEFAULT_POLICY_DIR.name == "governance"
    assert DEFAULT_POLICY_DIR.parent.name == "policies"


def test_none_policy_dir_uses_default_for_every_profile():
    result = load_governance_policy(None, _DEFAULTS)
    assert result["policy_dir"] is None
    for key, default in _DEFAULTS.items():
        assert result["profiles"][key] == default
        assert result["load_status"][key]["source"] == "built_in_default"
        assert result["load_status"][key]["reason"] == "no_policy_dir"


def test_missing_files_in_existing_dir_use_default_per_file(tmp_path):
    result = load_governance_policy(tmp_path, _DEFAULTS)
    for key, default in _DEFAULTS.items():
        assert result["profiles"][key] == default
        assert result["load_status"][key]["source"] == "built_in_default"
        assert result["load_status"][key]["reason"] == "file_not_found"


def test_present_file_overrides_default_for_that_profile_only(tmp_path):
    override = {"profile_id": "custom-thresholds", "thresholds": {"x": 99}}
    (tmp_path / THRESHOLDS_FILENAME).write_text(json.dumps(override), encoding="utf-8")

    result = load_governance_policy(tmp_path, _DEFAULTS)

    assert result["profiles"]["thresholds"] == override
    assert result["load_status"]["thresholds"]["source"] == "policy_file"
    assert result["load_status"]["thresholds"]["path"] == str(tmp_path / THRESHOLDS_FILENAME)
    # Every other profile still falls back to its own default.
    assert result["profiles"]["domain_policy"] == _DEFAULTS["domain_policy"]
    assert result["load_status"]["domain_policy"]["source"] == "built_in_default"


def test_all_five_profile_files_can_be_overridden_independently(tmp_path):
    for filename in (THRESHOLDS_FILENAME, DOMAIN_POLICY_FILENAME, CLIENT_ONBOARDING_FILENAME,
                      FINDING_RULES_FILENAME, ANOMALY_THRESHOLDS_FILENAME):
        (tmp_path / filename).write_text(json.dumps({"profile_id": filename}), encoding="utf-8")

    result = load_governance_policy(tmp_path, _DEFAULTS)

    assert result["profiles"]["thresholds"]["profile_id"] == THRESHOLDS_FILENAME
    assert result["profiles"]["domain_policy"]["profile_id"] == DOMAIN_POLICY_FILENAME
    assert result["profiles"]["client_onboarding"]["profile_id"] == CLIENT_ONBOARDING_FILENAME
    assert result["profiles"]["finding_rules"]["profile_id"] == FINDING_RULES_FILENAME
    assert result["profiles"]["anomaly_thresholds"]["profile_id"] == ANOMALY_THRESHOLDS_FILENAME
    assert all(s["source"] == "policy_file" for s in result["load_status"].values())


def test_present_but_malformed_json_raises_not_silently_falls_back(tmp_path):
    (tmp_path / THRESHOLDS_FILENAME).write_text("{not valid json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        load_governance_policy(tmp_path, _DEFAULTS)


def test_policy_dir_string_input_normalised_to_str_in_result(tmp_path):
    result = load_governance_policy(Path(tmp_path), _DEFAULTS)
    assert result["policy_dir"] == str(tmp_path)
