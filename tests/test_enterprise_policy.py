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
    assert (policy.enterprise_label, policy.source, policy.policy_path) == ("From File", "policy_file", str(config.resolve()))
    override = load_enterprise_policy(config, "From CLI")
    assert (override.enterprise_label, override.source) == ("From CLI", "cli_override")

def test_blank_override_rejected():
    with pytest.raises(ValueError):
        load_enterprise_policy(enterprise_label="  ")

def test_policy_provenance_records_effective_configuration(tmp_path):
    policy = load_enterprise_policy(enterprise_label="Deployment Enterprise")
    path = write_enterprise_policy_provenance(tmp_path, policy)
    assert json.loads(path.read_text()) == {
        "configuration_identifier": policy.configuration_identifier,
        "enterprise_business_center_token": "0000", "enterprise_label": "Deployment Enterprise",
        "schema": "enterprise_policy.v1", "source": "cli_override",
    }
    first = path.read_bytes()
    write_enterprise_policy_provenance(tmp_path, policy)
    assert path.read_bytes() == first == policy.provenance_bytes()
    assert b"private-deployment" not in first

def test_policy_instances_are_immutable_serializable_and_do_not_leak_state():
    import pickle
    alpha = load_enterprise_policy(enterprise_label="Enterprise Alpha")
    beta = load_enterprise_policy(enterprise_label="Enterprise Beta")
    assert alpha.is_enterprise("ENTERPRISE ALPHA")
    assert not beta.is_enterprise("Enterprise Alpha")
    assert pickle.loads(pickle.dumps(alpha)) == alpha
    with pytest.raises(AttributeError):
        alpha.enterprise_label = "mutated"


def test_malformed_schema_and_invalid_bookkeeping_token_are_rejected(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({"schema": "enterprise_policy.v0", "enterprise_label": "X"}))
    with pytest.raises(ValueError, match="schema"):
        load_enterprise_policy(path)
    path.write_text(json.dumps({"schema": "enterprise_policy.v1", "enterprise_label": "X", "enterprise_business_center_token": "9999"}))
    with pytest.raises(ValueError, match="0000"):
        load_enterprise_policy(path)


def test_is_enterprise_tolerates_non_string_values():
    policy = load_enterprise_policy()
    assert not policy.is_enterprise(None)
    assert not policy.is_enterprise(float("nan"))
    assert not policy.is_enterprise(123)


def test_policy_file_path_is_memory_only_provenance_is_safe(tmp_path):
    path = tmp_path / "private-deployment" / "enterprise.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"schema": "enterprise_policy.v1", "enterprise_label": "X"}))
    policy = load_enterprise_policy(path)
    assert policy.policy_path == str(path.resolve())
    assert str(tmp_path) not in json.dumps(policy.provenance())
