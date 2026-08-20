import json
import pytest
from tools.enterprise_policy import (
    DEFAULT_ENTERPRISE_LABEL, load_enterprise_policy, write_enterprise_policy_provenance,
)

def test_default_enterprise_label_is_synthetic():
    assert DEFAULT_ENTERPRISE_LABEL == "InternalEnterprise"
    assert load_enterprise_policy().source == "checked_in_default"

def test_policy_file_and_cli_precedence(tmp_path):
    config = tmp_path / "enterprise.json"
    config.write_text(json.dumps({"schema": "enterprise_policy.v1", "enterprise_label": "From File"}))
    policy = load_enterprise_policy(config)
    assert (policy.enterprise_label, policy.source, policy.policy_path) == ("From File", "policy_file", "enterprise.json")
    override = load_enterprise_policy(config, "From CLI")
    assert (override.enterprise_label, override.source) == ("From CLI", "cli_override")

def test_blank_override_rejected():
    with pytest.raises(ValueError):
        load_enterprise_policy(enterprise_label="  ")

def test_policy_provenance_records_effective_configuration(tmp_path):
    policy = load_enterprise_policy(enterprise_label="Deployment Enterprise")
    path = write_enterprise_policy_provenance(tmp_path, policy)
    assert json.loads(path.read_text()) == {
        "enterprise_business_center_token": "0000", "enterprise_label": "Deployment Enterprise",
        "schema": "enterprise_policy.v1", "source": "cli_override",
    }
