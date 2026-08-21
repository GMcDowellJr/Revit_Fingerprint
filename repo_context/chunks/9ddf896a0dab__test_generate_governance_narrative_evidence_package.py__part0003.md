# Chunk of tests/test_generate_governance_narrative_evidence_package.py

- Source relative path: `tests/test_generate_governance_narrative_evidence_package.py`
- Chunk: 3 of 3
- Original line range: 1036-1058
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_package_schema_version_override_is_consistent_across_manifest_health_and_evidence_map
- Source SHA-256: e60c8dba47b5674d967f6b921c70c80a38089dd30ac298318b6e62f873f624ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  1036| def test_package_schema_version_override_is_consistent_across_manifest_health_and_evidence_map(tmp_path, monkeypatch):
  1037|     """Regression test for a PR review finding: --package-schema-version was
  1038|     reflected in governance_package_manifest.json/_health.json's own top-level
  1039|     schema fields, but governance_evidence_map.json's entries describing those
  1040|     two files hard-coded the module default (PACKAGE_SCHEMA_VERSION) instead
  1041|     of the actual runtime override -- so a consumer following the evidence
  1042|     map to select a schema contract would pick the wrong one."""
  1043|     summary_path, pooled_path = _minimal_fixture(tmp_path)
  1044|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
  1045|                             "--out", str(tmp_path), "--package-schema-version", "2.0"])
  1046|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
  1047|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
  1048|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
  1049|     by_id = {a["artifact_id"]: a for a in evidence_map["artifacts"]}
  1050| 
  1051|     assert manifest["package_schema_version"] == "2.0"
  1052|     assert health["schema_version"] == "2.0"
  1053|     assert by_id["governance_package_manifest"]["schema_version"] == "2.0"
  1054|     assert by_id["governance_package_health"]["schema_version"] == "2.0"
  1055|     # governance_evidence_map.json's own schema (EVIDENCE_MAP_SCHEMA_VERSION) is a
  1056|     # separate versioning axis with no CLI override -- it must stay at its default.
  1057|     assert evidence_map["schema_version"] == EVIDENCE_MAP_SCHEMA_VERSION
  1058|     assert by_id["governance_evidence_map"]["schema_version"] == EVIDENCE_MAP_SCHEMA_VERSION
```
