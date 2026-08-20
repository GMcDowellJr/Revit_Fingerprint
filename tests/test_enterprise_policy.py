import json
from tools.enterprise_policy import DEFAULT_ENTERPRISE_LABEL, write_enterprise_policy_provenance

def test_default_enterprise_label_is_synthetic():
    assert DEFAULT_ENTERPRISE_LABEL == "InternalEnterprise"

def test_policy_provenance_records_effective_configuration(tmp_path):
    path = write_enterprise_policy_provenance(tmp_path, "Deployment Enterprise", "cli")
    assert json.loads(path.read_text()) == {
        "enterprise_business_center_token": "0000", "enterprise_label": "Deployment Enterprise",
        "schema": "enterprise_policy.v1", "source": "cli",
    }
