# tests/test_record_contract_v2.py
import json
import os

import pytest

from validators.record_v2 import validate_record_v2


@pytest.fixture(scope="session")
def domain_identity_registry_v2():
    with open("contracts/domain_identity_keys_v2.json", "r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def exported_fingerprint_json():
    """
    Provide exporter output JSON for validation.

    Options:
      1) Set env var FINGERPRINT_JSON_PATH to a file path.
      2) Replace this fixture in your Revit-runner harness to return in-memory export.
    """
    path = os.environ.get("FINGERPRINT_JSON_PATH")
    if not path:
        pytest.skip("Set FINGERPRINT_JSON_PATH to validate an exporter output JSON file.")
    with open(path, "r") as f:
        return json.load(f)


def test_all_exported_records_conform_to_record_contract_v2(
    exported_fingerprint_json,
    domain_identity_registry_v2,
):
    failures = []

    domains = exported_fingerprint_json.get("domains", {})
    assert isinstance(domains, dict) and domains, "export JSON missing domains dict"

    for domain, payload in domains.items():
        records = payload.get("records", [])
        if not isinstance(records, list):
            failures.append((domain, "<payload>", "payload.records.not_list"))
            continue

        for rec in records:
            rid = rec.get("record_id", "<no_record_id>")
            violations = validate_record_v2(rec, domain_identity_registry_v2)
            for v in violations:
                failures.append((domain, rid, v))

    assert not failures, (
        "Record contract violations:\n" +
        "\n".join(f"{d}:{rid}:{v}" for d, rid, v in failures)
    )


def test_blocked_records_have_no_sig_hash(exported_fingerprint_json):
    for domain, payload in exported_fingerprint_json.get("domains", {}).items():
        for rec in payload.get("records", []):
            if rec.get("status") == "blocked":
                assert rec.get("sig_hash") is None, f"{domain}:{rec.get('record_id')} blocked but sig_hash present"


def test_exported_records_have_unique_record_id_per_file_and_domain(exported_fingerprint_json):
    seen = set()
    dupes = []
    for domain, payload in exported_fingerprint_json.get("domains", {}).items():
        for rec in payload.get("records", []):
            key = (rec.get("file_id"), domain, rec.get("record_id"))
            if key in seen:
                dupes.append(key)
            seen.add(key)

    assert not dupes, f"duplicate record_id within (file_id, domain): {dupes}"
