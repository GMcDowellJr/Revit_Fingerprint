import json

import pytest

from core.deployment_config import load_deployment_config

CONTRACT = "contracts/domain_identity_keys_v2.json"
VALID_KEY = "project_info.business_center"


_DEFAULT = object()


def _write(tmp_path, fields=_DEFAULT, **overrides):
    payload = {
        "schema": "revit_fingerprint.deployment.v1",
        "project_info_shared_parameters": [] if fields is _DEFAULT else fields,
    }
    payload.update(overrides)
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_no_configuration_is_empty():
    assert load_deployment_config(None, CONTRACT) == {"project_info_shared_parameters": []}


def test_valid_name_only_configuration(tmp_path):
    fields = [{"key": VALID_KEY, "name": "Synthetic Field"}]
    assert load_deployment_config(_write(tmp_path, fields), CONTRACT)["project_info_shared_parameters"] == [
        {"key": VALID_KEY, "name": "Synthetic Field", "guid": None}
    ]


def test_valid_guid_is_canonicalized(tmp_path):
    fields = [{"key": VALID_KEY, "name": "Synthetic Field", "guid": "{AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE}"}]
    actual = load_deployment_config(_write(tmp_path, fields), CONTRACT)["project_info_shared_parameters"]
    assert actual[0]["guid"] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


@pytest.mark.parametrize("payload", [
    [], None, "bad", 1,
])
def test_rejects_non_object_top_level(tmp_path, payload):
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object"):
        load_deployment_config(path, CONTRACT)


@pytest.mark.parametrize("payload", [
    {"project_info_shared_parameters": []},
    {"schema": "wrong", "project_info_shared_parameters": []},
])
def test_rejects_missing_or_invalid_schema(tmp_path, payload):
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        load_deployment_config(path, CONTRACT)


@pytest.mark.parametrize("fields", [None, {}, "bad"])
def test_rejects_non_list_mapping(tmp_path, fields):
    with pytest.raises(ValueError, match="must be a list"):
        load_deployment_config(_write(tmp_path, fields), CONTRACT)


@pytest.mark.parametrize("fields", [
    ["bad"],
    [{"key": VALID_KEY, "name": " "}],
    [{"key": "other.field", "name": "X"}],
    [{"key": "project_info.number", "name": "X"}],
    [{"key": "project_info.unregistered", "name": "X"}],
    [{"key": VALID_KEY, "name": "X", "guid": "bad"}],
    [{"key": VALID_KEY, "name": "X"}, {"key": VALID_KEY, "name": "Y"}],
    [{"key": VALID_KEY, "name": "X", "extra": True}],
])
def test_rejects_invalid_mapping_entries(tmp_path, fields):
    with pytest.raises(ValueError):
        load_deployment_config(_write(tmp_path, fields), CONTRACT)


def test_rejects_duplicate_guid_mapped_to_conflicting_keys(tmp_path):
    contract = json.loads(open(CONTRACT, encoding="utf-8").read())
    contract["domains"]["identity"]["allowed_keys"].append("project_info.synthetic_secondary")
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    fields = [
        {"key": VALID_KEY, "name": "X", "guid": "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE"},
        {"key": "project_info.synthetic_secondary", "name": "Y", "guid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"},
    ]
    with pytest.raises(ValueError, match="conflicting keys"):
        load_deployment_config(_write(tmp_path, fields), contract_path)


def test_rejects_unknown_top_level_field(tmp_path):
    with pytest.raises(ValueError, match="unknown deployment"):
        load_deployment_config(_write(tmp_path, extension={}), CONTRACT)


def test_rejects_missing_mapping_field(tmp_path):
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps({"schema": "revit_fingerprint.deployment.v1"}), encoding="utf-8")
    with pytest.raises(ValueError, match="requires"):
        load_deployment_config(path, CONTRACT)


def test_missing_contract_fails(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_deployment_config(_write(tmp_path), tmp_path / "missing.json")


@pytest.mark.parametrize("contract", [{}, {"domains": {"identity": {"allowed_keys": "bad"}}}])
def test_malformed_contract_fails(tmp_path, contract):
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="contract"):
        load_deployment_config(_write(tmp_path), path)
