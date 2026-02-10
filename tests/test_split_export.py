# -*- coding: utf-8 -*-
"""Smoke tests for monolithic fingerprint exports."""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.features import build_features
from core.manifest import build_manifest


def _sample_monolithic() -> dict:
    return {
        "_contract": {
            "schema_version": "2.0",
            "run_status": "ok",
            "run_diag": {"errors": [], "counters": {"domain_total": 2, "domain_ok": 2}},
            "domains": {
                "identity": {
                    "domain": "identity",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "abc123",
                    "block_reasons": [],
                    "diag": {"count": 1, "raw_count": 1},
                },
                "units": {
                    "domain": "units",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "def456",
                    "block_reasons": [],
                    "diag": {"count": 5, "raw_count": 7},
                },
            },
        },
        "_hash_mode": "semantic",
        "identity": {
            "project_title": "Test Project",
            "is_workshared": False,
            "revit_version_number": "2024",
        },
        "units": {"records": [{"schema_version": "record.v2", "record_id": "U1"}]},
    }


def test_monolithic_manifest_surface():
    fp = _sample_monolithic()
    manifest = build_manifest(fp, include_identity=True)

    assert manifest["schema_version"] == "2.0"
    assert manifest["hash_mode"] == "semantic"
    assert manifest["domains"]["identity"]["hash"] == "abc123"
    assert manifest["identity"]["project_title"] == "Test Project"


def test_monolithic_features_surface():
    fp = _sample_monolithic()
    features = build_features(fp)

    assert features["schema_version"] == "2.0"
    assert features["run_status"] == "ok"
    assert features["domains"]["identity"]["count"] == 1
    assert features["domains"]["units"]["raw_count"] == 7
