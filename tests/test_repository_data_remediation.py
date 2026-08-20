"""Regression checks for repository-neutral runtime and sample configuration."""

import json
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def test_runner_install_discovery_has_only_generic_defaults():
    for path in ("runner/thin_runner.py", "runner/probe_thin_runner.py"):
        source = _read(path)
        assert 'REVIT_FINGERPRINT_ORG_DIR", "Company"' in source
        assert "userprofile:stantec_general_code" not in source.lower()
        assert "Revit_Fingerprint - General" not in source

    assert 'except Exception:\n    ORG_DIR = "Company"' in _read("runner/thin_runner.py")


def test_dynamo_graphs_embed_current_runners_without_workstation_paths():
    pairs = (
        ("Revit fingerprint MVP.dyn", "runner/thin_runner.py"),
        ("tools/probes/fingerprint_probe.dyn", "runner/probe_thin_runner.py"),
    )
    for graph_path, runner_path in pairs:
        graph = json.loads(_read(graph_path))
        embedded = [node["Code"] for node in graph["Nodes"] if node.get("Code") == _read(runner_path)]
        assert embedded == [_read(runner_path)]
        serialized = json.dumps(graph).lower()
        assert "c:\\\\users\\\\" not in serialized
        assert "onedrive -" not in serialized


def test_default_client_sector_policy_uses_synthetic_labels():
    policy = _read("policies/client_sector.csv")
    assert "ClientAlpha,healthcare" in policy
    assert "ClientEpsilon,semiconductor" in policy
    for organization in ("Kaiser", "Sutter", "Stantec", "Permanente"):
        assert organization.lower() not in policy.lower()
