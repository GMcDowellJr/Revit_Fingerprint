import json
from pathlib import Path

import domains.identity as identity
from runner.extraction_context import build_extraction_context, operator_deployment_config_path
from tests.test_identity_project_info import (
    FakeBuiltInParameter, FakeDoc, FakeGuid, FakeProjectInformation, _ALL_BUILTIN_VALUES,
)


def test_operator_environment_boundary():
    assert operator_deployment_config_path({"REVIT_FINGERPRINT_DEPLOYMENT_CONFIG": " /tmp/config.json "}) == "/tmp/config.json"
    assert operator_deployment_config_path({}) is None


def test_runner_loaded_mapping_reaches_identity_and_signature(tmp_path, monkeypatch):
    """Proves file -> real runner builder -> exact ctx -> identity item/hash path."""
    config = tmp_path / "deployment.json"
    config.write_text(json.dumps({
        "schema": "revit_fingerprint.deployment.v1",
        "project_info_shared_parameters": [{
            "key": "project_info.business_center",
            "name": "Synthetic Routing Field",
            "guid": "{AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE}",
        }],
    }), encoding="utf-8")
    ctx = build_extraction_context(Path(__file__).parents[1], config)
    assert ctx["project_info_shared_parameters"] == [{
        "key": "project_info.business_center",
        "name": "Synthetic Routing Field",
        "guid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    }]

    monkeypatch.setattr(identity, "BuiltInParameter", FakeBuiltInParameter)
    monkeypatch.setattr(identity, "Guid", FakeGuid)
    pi = FakeProjectInformation(
        "Synthetic Project", _ALL_BUILTIN_VALUES, set(),
        guid_values={"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee": "Unit 42"},
    )
    configured = identity.extract(FakeDoc(pi), ctx=ctx)["records"][0]
    baseline = identity.extract(FakeDoc(pi), ctx=build_extraction_context(Path(__file__).parents[1]))["records"][0]
    item = next(i for i in configured["identity_basis"]["items"] if i["k"] == "project_info.business_center")
    assert item == {"k": "project_info.business_center", "q": "ok", "v": "Unit 42"}
    assert item["k"] in configured["sig_basis"]["keys_used"]
    assert item["k"] not in baseline["sig_basis"]["keys_used"]
    assert configured["sig_hash"] != baseline["sig_hash"]
