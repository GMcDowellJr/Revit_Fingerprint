import json
import pytest
from core.deployment_config import load_deployment_config

CONTRACT = "contracts/domain_identity_keys_v2.json"

def _write(tmp_path, fields):
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps({"schema": "revit_fingerprint.deployment.v1", "project_info_shared_parameters": fields}))
    return path

def test_runner_loader_validates_registered_mapping(tmp_path):
    fields = [{"key": "project_info.business_center", "name": "Synthetic Field", "guid": "11111111-2222-4333-8444-555555555555"}]
    assert load_deployment_config(_write(tmp_path, fields), CONTRACT)["project_info_shared_parameters"] == fields

@pytest.mark.parametrize("fields", [
    [{"key": "project_info.number", "name": "Collision"}],
    [{"key": "project_info.unregistered", "name": "Unknown"}],
    [{"key": "project_info.business_center", "name": ""}],
    [{"key": "project_info.business_center", "name": "X", "guid": "bad"}],
])
def test_runner_loader_rejects_invalid_mapping(tmp_path, fields):
    with pytest.raises(ValueError):
        load_deployment_config(_write(tmp_path, fields), CONTRACT)
