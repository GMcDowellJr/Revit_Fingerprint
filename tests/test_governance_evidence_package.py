"""Unit tests for tools/governance_evidence_package.py: build_package_manifest,
build_package_health, build_evidence_map, comparison_type_coverage, write_json.

Pure tests against hand-built dicts -- no dependency on generate_governance_narrative.py
or its CLI. See docs/governance_evidence_package.md.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from governance_evidence_package import (  # noqa: E402
    AUTHORITY_LEVELS,
    EVIDENCE_MAP_SCHEMA_VERSION,
    GENERATOR_IDENTITY,
    GENERATOR_ROLE,
    PACKAGE_SCHEMA_VERSION,
    PACKAGE_TYPE,
    build_evidence_map,
    build_package_health,
    build_package_manifest,
    comparison_type_coverage,
    write_json,
)


# ---------------------------------------------------------------------------
# write_json
# ---------------------------------------------------------------------------

def test_write_json_round_trips(tmp_path):
    path = tmp_path / "out.json"
    payload = {"a": 1, "b": [1, 2, 3], "c": None}
    write_json(path, payload)
    assert json.loads(path.read_text(encoding="utf-8")) == payload
    assert path.read_text(encoding="utf-8").endswith("\n")


# ---------------------------------------------------------------------------
# comparison_type_coverage
# ---------------------------------------------------------------------------

def test_comparison_type_coverage_recognized_only():
    cov = comparison_type_coverage({"template_to_project", "container_to_project"},
                                    {"template_to_project", "container_to_project", "template_to_container"})
    assert cov["seen"] == ["container_to_project", "template_to_project"]
    assert cov["recognized"] == ["container_to_project", "template_to_project"]
    assert cov["intentionally_excluded"] == []
    assert cov["unrecognized"] == []


def test_comparison_type_coverage_flags_unrecognized():
    cov = comparison_type_coverage({"template_to_project", "bogus_type"}, {"template_to_project"})
    assert cov["unrecognized"] == ["bogus_type"]
    assert "bogus_type" not in cov["recognized"]


def test_comparison_type_coverage_intentionally_excluded_is_distinct_from_unrecognized():
    cov = comparison_type_coverage(
        {"sibling_templates", "template_to_project"},
        {"sibling_templates", "template_to_project"},
        intentionally_excluded={"sibling_templates"},
    )
    assert cov["intentionally_excluded"] == ["sibling_templates"]
    assert cov["recognized"] == ["template_to_project"]
    assert cov["unrecognized"] == []


def test_comparison_type_coverage_ignores_blank_values():
    cov = comparison_type_coverage({"", "template_to_project"}, {"template_to_project"})
    assert cov["seen"] == ["template_to_project"]


# ---------------------------------------------------------------------------
# build_package_manifest
# ---------------------------------------------------------------------------

def _manifest(**overrides):
    kwargs = dict(
        generator_identity=GENERATOR_IDENTITY,
        generator_role=GENERATOR_ROLE,
        package_schema_version=PACKAGE_SCHEMA_VERSION,
        analysis_date="2026-07-16",
        input_paths={"summary": Path("cross_segment_summary.csv"), "pooled": Path("cross_segment_pooled.csv")},
        input_required={"summary": True, "pooled": True},
        input_roles={"summary": "authoritative_deterministic_evidence", "pooled": "authoritative_deterministic_evidence"},
        output_paths={}, output_types={}, output_authority={}, output_context_role={},
        policy_dir=None, comparison_run_ids=[], source_executed_utc=[],
    )
    kwargs.update(overrides)
    return build_package_manifest(**kwargs)


def test_manifest_records_generator_identity_and_schema_version():
    m = _manifest()
    assert m["package_type"] == PACKAGE_TYPE
    assert m["generator"]["name"] == GENERATOR_IDENTITY
    assert m["package_schema_version"] == PACKAGE_SCHEMA_VERSION


def test_manifest_marks_input_present_based_on_real_filesystem_state(tmp_path):
    existing = tmp_path / "cross_segment_summary.csv"
    existing.write_text("x", encoding="utf-8")
    missing = tmp_path / "cross_segment_pooled.csv"
    m = _manifest(input_paths={"summary": existing, "pooled": missing})
    by_id = {i["artifact_id"]: i for i in m["inputs"]}
    assert by_id["summary"]["present"] is True
    assert by_id["pooled"]["present"] is False


def test_manifest_does_not_claim_missing_source_identifiers():
    m = _manifest(comparison_run_ids=[], source_executed_utc=[])
    assert m["corpus_scope"]["comparison_run_ids"] == []
    assert m["corpus_scope"]["source_executed_utc"] == []


def test_manifest_package_status_incomplete_when_required_input_missing(tmp_path):
    missing = tmp_path / "cross_segment_summary.csv"
    m = _manifest(input_paths={"summary": missing, "pooled": missing})
    assert m["package_status"] == "incomplete"


def test_manifest_package_status_complete_when_required_inputs_present(tmp_path):
    p1 = tmp_path / "a.csv"
    p1.write_text("x", encoding="utf-8")
    p2 = tmp_path / "b.csv"
    p2.write_text("x", encoding="utf-8")
    m = _manifest(input_paths={"summary": p1, "pooled": p2})
    assert m["package_status"] == "complete"


def test_manifest_records_output_sizes(tmp_path):
    out_csv = tmp_path / "governance_domain_summary.csv"
    out_csv.write_text("domain,tier\nline_styles,strong\n", encoding="utf-8")
    m = _manifest(
        output_paths={"governance_domain_summary": out_csv},
        output_types={"governance_domain_summary": "csv"},
        output_authority={"governance_domain_summary": "authoritative_deterministic_evidence"},
        output_context_role={"governance_domain_summary": "primary tier/score rollup"},
    )
    out = m["outputs"][0]
    assert out["artifact_id"] == "governance_domain_summary"
    assert out["present"] is True
    assert out["size_bytes"] == out_csv.stat().st_size


def test_manifest_records_policy_dir_as_inert_field():
    m = _manifest(policy_dir=Path("/some/policy/dir"))
    assert m["policy_profiles"]["policy_dir"] == "/some/policy/dir"


def test_manifest_without_policy_profiles_kwarg_keeps_pr1_not_yet_implemented_note():
    """A caller that hasn't adopted policy loading (policy_profiles omitted)
    must get PR1's original wording back -- this is generate_governance_narrative.py's
    own contract before PR3 wired --policy-dir in, and any other future caller
    of build_package_manifest() that doesn't pass policy_profiles."""
    m = _manifest()
    assert m["policy_profiles"]["profiles"] == {}
    assert "not yet implemented" in m["policy_profiles"]["note"]


def test_manifest_records_policy_profiles_when_supplied():
    policy_profiles = {
        "thresholds": {"profile_id": "governance-thresholds-v1", "schema_version": "0.1", "source": "policy_file"},
        "domain_policy": {"profile_id": "domain-governance-policy-v1", "schema_version": "0.1", "source": "built_in_default"},
    }
    m = _manifest(policy_dir=Path("/some/policy/dir"), policy_profiles=policy_profiles)
    assert m["policy_profiles"]["profiles"] == policy_profiles
    assert "not yet implemented" not in m["policy_profiles"]["note"]


# ---------------------------------------------------------------------------
# build_package_health
# ---------------------------------------------------------------------------

def _health(**overrides):
    kwargs = dict(
        schema_version=PACKAGE_SCHEMA_VERSION,
        schema_detection="dual",
        used_view_fallback=False,
        comparison_type_coverage_by_fn={
            "build_cascade": comparison_type_coverage({"template_to_project"}, {"template_to_project"}),
            "build_governance_state_summary": comparison_type_coverage(set(), set()),
        },
        required_inputs={"summary": True, "pooled": True},
        optional_inputs={"governance_states": False},
        client_sector_status="default_path_resolved",
        domain_csv_row_count=1, domain_rows_excluded_no_signal=0, client_csv_row_count=1,
        corpus_project_file_count=10, excluded_from_scoring=["view_templates_renderings_drafting"],
        unit_systems_seen=["imperial"], matrix_manifest_row_count=0, matrix_names_seen=[],
    )
    kwargs.update(overrides)
    return build_package_health(**kwargs)


def test_health_complete_when_all_required_present_and_no_warnings():
    h = _health()
    assert h["overall_status"] == "complete"
    assert h["blocking_conditions"] == []
    assert h["warnings"] == []


def test_health_invalid_when_required_input_missing():
    h = _health(required_inputs={"summary": True, "pooled": False})
    assert h["overall_status"] == "invalid"
    assert h["blocking_conditions"]
    assert h["blocking_conditions"][0]["condition"] == "missing_required_input"


def test_health_degraded_not_invalid_when_only_optional_signal_is_a_warning():
    h = _health(used_view_fallback=True)
    assert h["overall_status"] == "degraded"
    assert h["used_view_fallback"] is True
    assert "used_view_falls_back_to_legacy" in h["fallbacks_used"]


def test_health_reports_unrecognized_comparison_type_as_warning():
    cov = comparison_type_coverage({"bogus"}, {"template_to_project"})
    h = _health(comparison_type_coverage_by_fn={"build_cascade": cov, "build_governance_state_summary": comparison_type_coverage(set(), set())})
    assert h["overall_status"] == "degraded"
    conditions = [w["condition"] for w in h["warnings"]]
    assert "unrecognized_comparison_type" in conditions


def test_health_reports_client_sector_default_missing():
    h = _health(client_sector_status="default_path_missing")
    conditions = [w["condition"] for w in h["warnings"]]
    assert "client_sector_default_path_missing" in conditions


def test_health_reports_client_sector_explicit_missing():
    h = _health(client_sector_status="explicit_path_missing")
    conditions = [w["condition"] for w in h["warnings"]]
    assert "client_sector_explicit_path_missing" in conditions


def test_health_omitted_policy_load_status_adds_no_warning_and_stays_complete():
    """A caller that hasn't adopted policy loading (policy_load_status
    omitted) must get identical health output to before this parameter
    existed."""
    h = _health()
    assert h["policy_load_status"] == {}
    assert h["overall_status"] == "complete"


def test_health_reports_policy_profile_defaulted_as_warning_and_degraded():
    h = _health(policy_load_status={
        "thresholds": {"source": "policy_file", "path": "/x/governance_thresholds.json", "reason": None},
        "domain_policy": {"source": "built_in_default", "path": "/x/domain_governance_policy.json", "reason": "file_not_found"},
    })
    assert h["overall_status"] == "degraded"
    assert "governance_policy_built_in_default" in h["fallbacks_used"]
    conditions = [w["condition"] for w in h["warnings"]]
    assert "governance_policy_profile_defaulted" in conditions


def test_health_all_policy_profiles_from_file_adds_no_warning():
    h = _health(policy_load_status={
        "thresholds": {"source": "policy_file", "path": "/x/governance_thresholds.json", "reason": None},
        "domain_policy": {"source": "policy_file", "path": "/x/domain_governance_policy.json", "reason": None},
    })
    assert h["overall_status"] == "complete"
    assert h["fallbacks_used"] == []


def test_health_does_not_warn_when_client_sector_explicitly_provided():
    h = _health(client_sector_status="explicit_path")
    conditions = [w["condition"] for w in h["warnings"]]
    assert "client_sector_default_path_missing" not in conditions
    assert "client_sector_explicit_path_missing" not in conditions


def test_health_matrix_manifest_reports_row_count_and_names():
    h = _health(matrix_manifest_row_count=3, matrix_names_seen=["project_union_jaccard_matrix"])
    assert h["matrix_manifest"]["present"] is True
    assert h["matrix_manifest"]["row_count"] == 3
    assert h["matrix_manifest"]["matrix_names_seen"] == ["project_union_jaccard_matrix"]


def test_health_warning_and_limitation_text_has_no_severity_language():
    """Task-spec decision: known_limitations/warnings text must be mechanical/
    factual only, citing code behavior, never an impact/severity judgment."""
    h = _health(used_view_fallback=True, client_sector_status="default_path_missing",
                comparison_type_coverage_by_fn={
                    "build_cascade": comparison_type_coverage({"bogus"}, {"template_to_project"}),
                    "build_governance_state_summary": comparison_type_coverage(set(), set()),
                })
    denylist = ("may cause", "risk", "misleading", "could lead to", "concerning", "problematic")
    for w in h["warnings"]:
        lowered = w["detail"].lower()
        for term in denylist:
            assert term not in lowered, f"severity language '{term}' found in: {w['detail']}"


# ---------------------------------------------------------------------------
# build_evidence_map
# ---------------------------------------------------------------------------

def _evidence_map(**overrides):
    kwargs = dict(
        schema_version=EVIDENCE_MAP_SCHEMA_VERSION,
        input_paths={"summary": Path("cross_segment_summary.csv"), "pooled": Path("cross_segment_pooled.csv")},
        input_present={"summary": True, "pooled": True},
        output_paths={"governance_domain_summary": Path("governance_domain_summary.csv")},
        sibling_paths={
            "file_pairs": Path("cross_segment_file_pairs.csv"),
            "comparison_registry": Path("comparison_registry.csv"),
        },
        sibling_present={"file_pairs": False, "comparison_registry": False},
    )
    kwargs.update(overrides)
    return build_evidence_map(**kwargs)


def test_evidence_map_has_twenty_eight_unique_artifacts():
    em = _evidence_map()
    ids = [a["artifact_id"] for a in em["artifacts"]]
    assert len(ids) == 28
    assert "governance_findings" in ids
    assert len(ids) == len(set(ids))


def test_evidence_map_required_fields_populated_for_every_artifact():
    em = _evidence_map()
    required_keys = {
        "artifact_id", "path", "artifact_type", "required", "producer",
        "authority_level", "context_role", "grain", "can_answer",
        "cannot_answer", "known_limitations", "null_semantics", "related_artifacts",
    }
    for a in em["artifacts"]:
        missing = required_keys - set(a.keys())
        assert not missing, f"{a['artifact_id']} missing keys: {missing}"
        assert a["artifact_type"], a["artifact_id"]
        assert a["authority_level"], a["artifact_id"]
        assert a["grain"], a["artifact_id"]


def test_evidence_map_authority_levels_use_only_defined_vocabulary():
    em = _evidence_map()
    for a in em["artifacts"]:
        assert a["authority_level"] in AUTHORITY_LEVELS, a["artifact_id"]


def test_evidence_map_narrative_is_controlled_interpretation_not_authoritative():
    em = _evidence_map()
    narrative = next(a for a in em["artifacts"] if a["artifact_id"] == "governance_narrative_context")
    assert narrative["authority_level"] == "controlled_interpretation"


def test_evidence_map_source_csvs_are_authoritative():
    em = _evidence_map()
    for artifact_id in ("cross_segment_summary", "cross_segment_pooled",
                        "governance_domain_summary", "governance_client_summary"):
        entry = next(a for a in em["artifacts"] if a["artifact_id"] == artifact_id)
        assert entry["authority_level"] == "authoritative_deterministic_evidence", artifact_id


def test_evidence_map_client_sector_is_user_provided_note():
    em = _evidence_map()
    entry = next(a for a in em["artifacts"] if a["artifact_id"] == "client_sector")
    assert entry["authority_level"] == "user_provided_note"


def test_evidence_map_related_artifacts_are_valid_artifact_ids():
    em = _evidence_map()
    ids = {a["artifact_id"] for a in em["artifacts"]}
    for a in em["artifacts"]:
        for related in a["related_artifacts"]:
            assert related in ids, f"{a['artifact_id']} -> unknown related_artifacts entry {related!r}"
            assert "." not in related, f"{a['artifact_id']} -> related_artifacts entry looks like a filename: {related!r}"


def test_evidence_map_self_lists_all_other_artifacts():
    em = _evidence_map()
    self_entry = next(a for a in em["artifacts"] if a["artifact_id"] == "governance_evidence_map")
    other_ids = {a["artifact_id"] for a in em["artifacts"] if a["artifact_id"] != "governance_evidence_map"}
    assert set(self_entry["related_artifacts"]) == other_ids


def test_evidence_map_sibling_artifacts_present_flag_reflects_filesystem(tmp_path):
    file_pairs = tmp_path / "cross_segment_file_pairs.csv"
    file_pairs.write_text("not,parsed,by,generator\n", encoding="utf-8")
    missing_registry = tmp_path / "comparison_registry.csv"
    em = _evidence_map(
        sibling_paths={"file_pairs": file_pairs, "comparison_registry": missing_registry},
        sibling_present={"file_pairs": True, "comparison_registry": False},
    )
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["cross_segment_file_pairs"]["present"] is True
    assert by_id["comparison_registry"]["present"] is False
    # Sibling artifacts are archive-only: never opened/parsed by this generator.
    assert by_id["cross_segment_file_pairs"]["required"] is False
    assert by_id["comparison_registry"]["required"] is False


def test_evidence_map_uses_overridden_package_schema_version_for_manifest_and_health_entries():
    em = _evidence_map(package_schema_version="2.0")
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["governance_package_manifest"]["schema_version"] == "2.0"
    assert by_id["governance_package_health"]["schema_version"] == "2.0"
    # The evidence map's own schema (a separate versioning axis) is unaffected.
    assert by_id["governance_evidence_map"]["schema_version"] == EVIDENCE_MAP_SCHEMA_VERSION


def test_evidence_map_defaults_manifest_and_health_schema_version_to_package_default():
    em = _evidence_map()
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert by_id["governance_package_manifest"]["schema_version"] == PACKAGE_SCHEMA_VERSION
    assert by_id["governance_package_health"]["schema_version"] == PACKAGE_SCHEMA_VERSION


def test_evidence_map_known_limitations_text_has_no_severity_language():
    em = _evidence_map()
    denylist = ("may cause", "risk", "misleading", "could lead to", "concerning", "problematic")
    for a in em["artifacts"]:
        text = " ".join(a["known_limitations"]) if isinstance(a["known_limitations"], list) else str(a["known_limitations"])
        lowered = text.lower()
        for term in denylist:
            assert term not in lowered, f"severity language '{term}' found in {a['artifact_id']}: {text}"


def test_domain_and_client_summary_null_semantics_cite_the_actual_em_dash():
    """Regression test for a PR review finding: null_semantics claimed an
    ASCII hyphen '-' is the missing-value marker, but fmt()/pct() in
    generate_governance_narrative.py actually write the em dash '—'
    (U+2014) for a present-but-None numeric cell. A consumer normalizing the
    CSV by this metadata must be told the real character."""
    em = _evidence_map()
    for artifact_id in ("governance_domain_summary", "governance_client_summary"):
        entry = next(a for a in em["artifacts"] if a["artifact_id"] == artifact_id)
        text = " ".join(entry["null_semantics"].values())
        assert "—" in text, f"{artifact_id}.null_semantics does not mention the actual em dash: {text}"
