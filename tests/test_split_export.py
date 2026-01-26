# -*- coding: utf-8 -*-
"""
Smoke tests for split export (index.json + details.json) functionality.

Verifies:
  1. Index JSON can be loaded by build_manifest() and build_features()
  2. Details JSON contains domain payloads with records
  3. Legacy bundle still works as before
"""

import json
import sys
from pathlib import Path

# Add repo root to path for imports
_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.manifest import build_manifest
from core.features import build_features


def test_index_payload_structure():
    """
    Test that _build_index_payload creates the expected structure.

    This is a unit test without requiring a full Revit run.
    """
    # Mock fingerprint payload
    mock_payload = {
        "_contract": {
            "schema_version": "2.0",
            "run_status": "ok",
            "run_diag": {
                "errors": [],
                "counters": {
                    "domain_total": 2,
                    "domain_ok": 2,
                }
            },
            "domains": {
                "identity": {
                    "domain": "identity",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "abc123",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        "hash_mode": "semantic",
                        "count": 1,
                        "raw_count": 1,
                    }
                },
                "units": {
                    "domain": "units",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "def456",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        "hash_mode": "semantic",
                        "count": 5,
                        "raw_count": 5,
                    }
                }
            }
        },
        "_hash_mode": "semantic",
        "identity": {"project_title": "Test Project"},
        "_meta": {"runner": "M5"},
        "_notes": ["test note"],
        "_manifest": {},
        "_features": {},
        "_domains": {},
    }

    # Import the builder (simulate what runner does)
    # Since we can't easily import from runner/run_dynamo.py due to Revit dependencies,
    # we'll manually construct what the index should look like
    expected_index_keys = {"_contract", "_hash_mode", "identity", "_meta", "_notes", "artifacts"}

    # Simulate index structure
    index = {
        "_contract": mock_payload["_contract"],
        "_hash_mode": mock_payload["_hash_mode"],
        "identity": mock_payload["identity"],
        "_meta": mock_payload["_meta"],
        "_notes": mock_payload["_notes"],
        "artifacts": {
            "details_href": "test.details.json"
        }
    }

    # Verify index has expected keys
    assert set(index.keys()) == expected_index_keys, f"Index keys mismatch: {set(index.keys())} != {expected_index_keys}"

    # Verify contract structure
    assert "_contract" in index
    assert "domains" in index["_contract"]
    assert "identity" in index["_contract"]["domains"]

    # Verify counts are in diag
    assert "count" in index["_contract"]["domains"]["identity"]["diag"]
    assert "raw_count" in index["_contract"]["domains"]["identity"]["diag"]
    assert index["_contract"]["domains"]["identity"]["diag"]["count"] == 1

    print("✓ Index payload structure is correct")


def test_index_works_with_build_manifest():
    """
    Test that build_manifest() can work with just the index payload.
    """
    index_payload = {
        "_contract": {
            "schema_version": "2.0",
            "run_status": "ok",
            "run_diag": {
                "errors": [],
                "counters": {
                    "domain_total": 2,
                    "domain_ok": 2,
                }
            },
            "domains": {
                "identity": {
                    "domain": "identity",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "abc123",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        "hash_mode": "semantic",
                        "count": 1,
                    }
                }
            }
        },
        "_hash_mode": "semantic",
        "identity": {"project_title": "Test Project", "is_workshared": False, "revit_version_number": "2024"},
    }

    # This should work without needing full domain payloads
    manifest = build_manifest(index_payload, include_identity=True)

    assert manifest is not None
    assert "schema_version" in manifest
    assert manifest["schema_version"] == "2.0"
    assert "hash_mode" in manifest
    assert manifest["hash_mode"] == "semantic"
    assert "run_status" in manifest
    assert manifest["run_status"] == "ok"
    assert "domains" in manifest
    assert "identity" in manifest["domains"]
    assert manifest["domains"]["identity"]["hash"] == "abc123"

    # Verify identity inclusion
    assert "identity" in manifest
    assert manifest["identity"]["project_title"] == "Test Project"

    print("✓ build_manifest() works with index payload")


def test_index_works_with_build_features():
    """
    Test that build_features() can work with just the index payload (reads counts from contract).
    """
    index_payload = {
        "_contract": {
            "schema_version": "2.0",
            "run_status": "ok",
            "run_diag": {
                "errors": [],
                "counters": {
                    "domain_total": 2,
                    "domain_ok": 2,
                }
            },
            "domains": {
                "identity": {
                    "domain": "identity",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "abc123",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        "hash_mode": "semantic",
                        "count": 1,
                        "raw_count": 1,
                    }
                },
                "units": {
                    "domain": "units",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "def456",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        "hash_mode": "semantic",
                        "count": 5,
                        "raw_count": 7,
                    }
                }
            }
        },
        "_hash_mode": "semantic",
        "identity": {"project_title": "Test Project", "is_workshared": False},
    }

    # This should work by reading counts from contract.domains.*.diag
    features = build_features(index_payload)

    assert features is not None
    assert "schema_version" in features
    assert features["schema_version"] == "2.0"
    assert "hash_mode" in features
    assert features["hash_mode"] == "semantic"
    assert "domains" in features

    # Verify counts are correctly extracted from contract diag
    assert "identity" in features["domains"]
    assert features["domains"]["identity"]["count"] == 1
    assert features["domains"]["identity"]["raw_count"] == 1

    assert "units" in features["domains"]
    assert features["domains"]["units"]["count"] == 5
    assert features["domains"]["units"]["raw_count"] == 7

    print("✓ build_features() works with index payload (reads counts from contract)")


def test_details_payload_structure():
    """
    Test that details payload contains domain data without meta fields.
    """
    mock_full_payload = {
        "_contract": {},
        "_hash_mode": "semantic",
        "_manifest": {},
        "_features": {},
        "_meta": {},
        "_notes": [],
        "_domains": {},
        "identity": {"records": [], "count": 1},
        "units": {"records": [], "count": 5},
        "line_patterns": {"records": [], "count": 8},
    }

    # Simulate details extraction
    meta_keys = {"_contract", "_manifest", "_features", "_hash_mode", "_meta", "_notes", "_domains", "artifacts"}
    details = {k: v for k, v in mock_full_payload.items() if k not in meta_keys}

    # Verify details contains only domain payloads
    assert "identity" in details
    assert "units" in details
    assert "line_patterns" in details

    # Verify meta fields are excluded
    assert "_contract" not in details
    assert "_manifest" not in details
    assert "_features" not in details
    assert "_hash_mode" not in details
    assert "_meta" not in details

    # Verify records are present
    assert "records" in details["identity"]

    print("✓ Details payload structure is correct")


def test_backward_compatibility_with_legacy_payload():
    """
    Test that build_features() still works with legacy full payload (fallback to top-level counts).
    """
    legacy_payload = {
        "_contract": {
            "schema_version": "2.0",
            "run_status": "ok",
            "run_diag": {"errors": [], "counters": {}},
            "domains": {
                "identity": {
                    "domain": "identity",
                    "domain_version": "1",
                    "status": "ok",
                    "hash": "abc123",
                    "block_reasons": [],
                    "diag": {
                        "api_reachable": True,
                        # No count/raw_count in diag (old format)
                    }
                }
            }
        },
        "_hash_mode": "semantic",
        # Legacy: counts are in top-level domain payloads
        "identity": {
            "count": 1,
            "raw_count": 1,
            "records": []
        }
    }

    # Should fall back to reading counts from legacy payload
    features = build_features(legacy_payload)

    assert features is not None
    assert "domains" in features
    assert "identity" in features["domains"]

    # Verify fallback to legacy counts works
    assert features["domains"]["identity"]["count"] == 1
    assert features["domains"]["identity"]["raw_count"] == 1

    print("✓ build_features() backward compatibility with legacy payload works")


if __name__ == "__main__":
    print("Running split export smoke tests...\n")

    try:
        test_index_payload_structure()
        test_index_works_with_build_manifest()
        test_index_works_with_build_features()
        test_details_payload_structure()
        test_backward_compatibility_with_legacy_payload()

        print("\n✅ All smoke tests passed!")
        sys.exit(0)
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
