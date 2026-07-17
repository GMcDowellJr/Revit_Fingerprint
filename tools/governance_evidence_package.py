"""
governance_evidence_package.py

Package-boundary layer for tools/generate_governance_narrative.py.

Builds the JSON manifest/health/evidence-map artifacts that make the
governance narrative package's provenance, coverage, and navigation
structure machine-legible, without touching any of the generator's
deterministic calculations, thresholds, or CSV columns. This is Phase 1
("PR1") of a broader evidence-package refactor; structured findings
(governance_findings.json) and policy externalization are deferred to
later phases -- see docs/governance_evidence_package.md.

Design reference only: the authority-level vocabulary below is modeled on,
but independently defined from, the discovery-scaffold vocabulary in the
GMcDowellJr/llm_evidence_framework repository. That repository is explicitly
not a finalized standard or schema, and this module does not import from it
or depend on it at runtime -- these constants are this repo's own copy,
chosen for cross-tool legibility.

All text fields (known_limitations, cannot_answer, etc.) in this module are
mechanical/factual statements about what the code does, citing a specific
function, line, or docs/governance_narrative_scope_gap_audit.md finding ID
where relevant -- never an interpretive judgment about impact or severity.
Severity/impact judgment belongs to a human reader or to a future
governance_findings.json (PR2), not to this deterministic layer.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── package identity / schema versions ──────────────────────────────────────

PACKAGE_TYPE = "governance_evidence_package"
PACKAGE_SCHEMA_VERSION = "1.0"
EVIDENCE_MAP_SCHEMA_VERSION = "1.0"
FINDINGS_SCHEMA_VERSION = "1.0"

GENERATOR_IDENTITY = "generate_governance_narrative.py"
GENERATOR_ROLE = "deterministic_governance_narrative_generator"

# ── authority-level vocabulary ───────────────────────────────────────────────

AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE = "authoritative_deterministic_evidence"
AUTHORITY_CONTROLLED_INTERPRETATION = "controlled_interpretation"
AUTHORITY_CONVENIENCE_SUMMARY = "convenience_summary"
AUTHORITY_USER_PROVIDED_NOTE = "user_provided_note"
AUTHORITY_LLM_GENERATED_PROVISIONAL_INTERPRETATION = "llm_generated_provisional_interpretation"

AUTHORITY_LEVELS = {
    AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
    AUTHORITY_CONTROLLED_INTERPRETATION,
    AUTHORITY_CONVENIENCE_SUMMARY,
    AUTHORITY_USER_PROVIDED_NOTE,
    AUTHORITY_LLM_GENERATED_PROVISIONAL_INTERPRETATION,
}

# ── finding provenance vocabulary (epistemic provenance: origin/fidelity/ ────
# ── authority/limits) ─────────────────────────────────────────────────────────
# Names match the framework's four components of epistemic provenance
# (patterns/deterministic_to_llm_boundary.md in the design-reference-only
# llm_evidence_framework repo). Every finding in governance_findings.json is
# derived from deterministic computation over already-authoritative CSV data
# (build_cascade()/build_client_summary()/assign_tier() outputs), so origin
# and fidelity are constant across all findings this generator produces.

FINDING_ORIGIN_DETERMINISTIC_COMPUTATION = "deterministic_computation"
FINDING_FIDELITY_EXACT = "exact"
FINDING_STATUS_SUPPORTED = "supported"
FINDING_STATUS_QUESTION_NOT_CLAIM = "question_not_claim"

FINDING_TYPES = {
    "baseline_candidate",
    "strong_baseline_candidate",
    "local_review_required",
    "high_fragmentation",
    "active_local_practice",
    "cross_client_convergence",
    "low_client_coherence",
    "passive_inheritance_risk",
    "missing_or_degraded_evidence",
    "leadership_question",
}


def _utc_now_iso() -> str:
    # Matches compare_cross_segment.py's own executed_utc stamping convention
    # (see its `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`).
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def build_findings_document(findings: list, schema_version: str = FINDINGS_SCHEMA_VERSION) -> dict:
    """Wrap a list of finding dicts (already built by the caller -- domain-
    governance classification logic stays in generate_governance_narrative.py,
    which owns TIER_*/PASSIVE_INHERITANCE_RISK_DOMAINS/assign_tier()) in the
    same schema_version-tagged envelope used by the other three package
    artifacts. Pure function -- no filesystem I/O, no re-derivation of the
    findings themselves.
    """
    for f in findings:
        if f.get("finding_type") not in FINDING_TYPES:
            raise ValueError(f"unknown finding_type: {f.get('finding_type')!r}")
    return {"schema_version": schema_version, "findings": findings}


# ── package manifest ─────────────────────────────────────────────────────────

def build_package_manifest(
    *,
    generator_identity: str,
    generator_role: str,
    package_schema_version: str,
    analysis_date: str,
    input_paths: dict,          # artifact_id -> Optional[Path]
    input_required: dict,       # artifact_id -> bool
    input_roles: dict,          # artifact_id -> authority_level string
    output_paths: dict,         # artifact_id -> Path (already written to disk)
    output_types: dict,         # artifact_id -> artifact_type string
    output_authority: dict,     # artifact_id -> authority_level string
    output_context_role: dict,  # artifact_id -> context_role string
    policy_dir: Optional[Path],
    comparison_run_ids: list,
    source_executed_utc: list,
    policy_profiles: Optional[dict] = None,
) -> dict:
    """Pure function: reads only Path.exists()/Path.stat() for already-written
    output files. Does not open or parse any input CSV. Never claims a content
    hash or source-run identifier that isn't actually present in the loaded
    rows (comparison_run_ids/source_executed_utc are read from those rows by
    the caller, not invented here).

    policy_profiles: optional {"thresholds": {"profile_id":..., "schema_version":...,
    "source": "policy_file"|"built_in_default"}, "domain_policy": {...}, ...} --
    the resolved profile_id/schema_version/source tools/governance_policy.py's
    load_governance_policy() actually used for this run, one entry per policy
    profile (thresholds, domain_policy, client_onboarding, finding_rules).
    Omitted (None) reproduces PR1's original "not yet read" wording for a
    caller that hasn't adopted policy loading -- callers built on
    generate_governance_narrative.py's PR3 always pass this.
    """
    inputs = []
    for artifact_id, path in input_paths.items():
        present = bool(path) and path.exists()
        inputs.append({
            "artifact_id": artifact_id,
            "path": str(path) if path else None,
            "required": bool(input_required.get(artifact_id, False)),
            "present": present,
            "role": input_roles.get(artifact_id, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE),
        })

    outputs = []
    for artifact_id, path in output_paths.items():
        exists = path.exists()
        outputs.append({
            "artifact_id": artifact_id,
            "path": str(path),
            "artifact_type": output_types.get(artifact_id, "unknown"),
            "authority_level": output_authority.get(artifact_id),
            "context_role": output_context_role.get(artifact_id, ""),
            "present": exists,
            "size_bytes": path.stat().st_size if exists else None,
        })

    missing_required = [i["artifact_id"] for i in inputs if i["required"] and not i["present"]]

    return {
        "package_type": PACKAGE_TYPE,
        "package_schema_version": package_schema_version,
        "generated_at": _utc_now_iso(),
        "analysis_date": analysis_date,
        "generator": {
            "name": generator_identity,
            "role": generator_role,
        },
        "inputs": inputs,
        "outputs": outputs,
        "corpus_scope": {
            "comparison_run_ids": comparison_run_ids,
            "source_executed_utc": source_executed_utc,
        },
        "policy_profiles": {
            "policy_dir": str(policy_dir) if policy_dir else None,
            "profiles": policy_profiles or {},
            "note": (
                "Governance thresholds, domain-governance policy (excluded/"
                "passive-inheritance-risk domains, domain guidance text), "
                "client-onboarding interpretation thresholds, and finding-rule "
                "documentation are loaded from --policy-dir (default: "
                "policies/governance/) via tools/governance_policy.py at run "
                "time -- see the `profiles` field above for which profile_id/"
                "schema_version/source (policy_file vs. built_in_default) was "
                "actually used for each of the four profiles this run. "
                "policies/governance/*.json ship with values that reproduce "
                "this generator's pre-externalization Python literals exactly; "
                "overriding --policy-dir with a different profile set changes "
                "classification output."
            ) if policy_profiles else (
                "Policy externalization (thresholds, domain-governance policy, "
                "onboarding rules) is not yet implemented in this generator -- "
                "deferred to a future PR. This field records the --policy-dir "
                "value given, if any, for forward-compatibility auditing only; "
                "it is not read by this generator."
            ),
        },
        "package_status": "incomplete" if missing_required else "complete",
    }


# ── comparison-type coverage (shared shape) ──────────────────────────────────

def comparison_type_coverage(
    seen: set,
    known: set,
    intentionally_excluded: Optional[set] = None,
) -> dict:
    """Pure classification of a set of observed comparison_type values against
    a known/intentionally-excluded vocabulary. Shape matches the task spec's
    package-health comparison_type_coverage schema:
      seen / recognized / intentionally_excluded / unrecognized.
    """
    intentionally_excluded = intentionally_excluded or set()
    seen_clean = {s for s in seen if s}
    unrecognized = seen_clean - known
    excluded_seen = seen_clean & intentionally_excluded
    recognized = (seen_clean & known) - excluded_seen
    return {
        "seen": sorted(seen_clean),
        "recognized": sorted(recognized),
        "intentionally_excluded": sorted(excluded_seen),
        "unrecognized": sorted(unrecognized),
    }


# ── package health ────────────────────────────────────────────────────────────

def build_package_health(
    *,
    schema_version: str,
    schema_detection: str,               # "dual" | "single" | "none"
    used_view_fallback: bool,
    comparison_type_coverage_by_fn: dict,  # {"build_cascade": {...}, "build_governance_state_summary": {...}}
    required_inputs: dict,               # artifact_id -> bool present
    optional_inputs: dict,               # artifact_id -> bool present
    client_sector_status: str,           # "explicit_path" | "default_path_resolved" |
                                          # "default_path_missing" | "explicit_path_missing"
    domain_csv_row_count: int,
    domain_rows_excluded_no_signal: int,
    client_csv_row_count: int,
    corpus_project_file_count: int,
    excluded_from_scoring: list,
    unit_systems_seen: list,
    matrix_manifest_row_count: int,
    matrix_names_seen: list,
    policy_load_status: Optional[dict] = None,  # tools/governance_policy.py's load_status
) -> dict:
    """All text below is mechanical/factual only -- see module docstring.

    policy_load_status: optional {"thresholds": {"source": "policy_file"|
    "built_in_default", "path":..., "reason":...}, "domain_policy": {...}, ...}
    -- load_governance_policy()'s per-profile load_status for this run.
    Omitted (None, the default) adds no policy-related warning, so a caller
    that hasn't adopted policy loading gets identical health output to
    before this parameter existed.
    """
    blocking_conditions = []
    missing_required = sorted(k for k, present in required_inputs.items() if not present)
    if missing_required:
        blocking_conditions.append({
            "condition": "missing_required_input",
            "detail": f"Required input(s) not present: {missing_required}",
        })

    warnings = []
    fallbacks_used = []
    if used_view_fallback:
        fallbacks_used.append("used_view_falls_back_to_legacy")
        warnings.append({
            "condition": "used_view_fallback",
            "detail": (
                "used_view_falls_back_to_legacy() returned True: canonical "
                "used-view columns resolved to legacy all-view column names "
                "in _SUMMARY_COL_ALIASES."
            ),
        })
    for fn_name, coverage in comparison_type_coverage_by_fn.items():
        if coverage.get("unrecognized"):
            warnings.append({
                "condition": "unrecognized_comparison_type",
                "detail": (
                    f"{fn_name}: comparison_type value(s) not in the known "
                    f"vocabulary for that function: {coverage['unrecognized']}"
                ),
            })
    if client_sector_status == "default_path_missing":
        warnings.append({
            "condition": "client_sector_default_path_missing",
            "detail": (
                "The default --client-sector path (policies/client_sector.csv) "
                "does not exist on disk; every client_label is treated as "
                "unclassified sector by load_client_sectors()."
            ),
        })
    elif client_sector_status == "explicit_path_missing":
        warnings.append({
            "condition": "client_sector_explicit_path_missing",
            "detail": (
                "--client-sector was given an explicit path that does not "
                "exist; every client_label is treated as unclassified sector "
                "by load_client_sectors()."
            ),
        })

    policy_load_status = policy_load_status or {}
    policy_profiles_defaulted = sorted(
        name for name, status in policy_load_status.items()
        if status.get("source") == "built_in_default"
    )
    if policy_profiles_defaulted:
        fallbacks_used.append("governance_policy_built_in_default")
        warnings.append({
            "condition": "governance_policy_profile_defaulted",
            "detail": (
                "Governance policy profile(s) not found under --policy-dir; "
                f"this generator's own built-in default was used instead: "
                f"{policy_profiles_defaulted}. See governance_package_manifest.json's "
                "policy_profiles.profiles for the resolved profile_id/schema_version "
                "of each profile actually applied."
            ),
        })

    if missing_required:
        overall_status = "invalid"
    elif warnings:
        overall_status = "degraded"
    else:
        overall_status = "complete"

    return {
        "schema_version": schema_version,
        "overall_status": overall_status,
        "required_inputs": required_inputs,
        "optional_inputs": optional_inputs,
        "schema_detection": schema_detection,
        "used_view_fallback": used_view_fallback,
        "fallbacks_used": fallbacks_used,
        "comparison_type_coverage": comparison_type_coverage_by_fn,
        "client_sector_status": client_sector_status,
        "policy_load_status": policy_load_status,
        "scope_coverage": {
            "unit_systems_seen": unit_systems_seen,
            "note": (
                "Detailed scope-blending/comparability gating (deterministic "
                "comparable/weakly_comparable/not_comparable statuses) is "
                "deferred to a future PR; this field is a factual inventory "
                "of unit_system values observed in cross_segment_summary.csv "
                "only."
            ),
        },
        "matrix_manifest": {
            "present": matrix_manifest_row_count > 0,
            "row_count": matrix_manifest_row_count,
            "matrix_names_seen": matrix_names_seen,
            "note": (
                "matrix_output_manifest.csv (MATRIX_MANIFEST_FIELDS in "
                "compare_cross_segment.py) has no structured block/status "
                "column today -- only matrix_name/known_limitations/"
                "interpretation free-text fields. This generator does not "
                "parse or classify per-matrix blocking status; see "
                "docs/governance_generator_cross_compare_coverage.md."
            ),
        },
        "domain_csv_row_count": domain_csv_row_count,
        "domain_rows_excluded_no_signal": domain_rows_excluded_no_signal,
        "client_csv_row_count": client_csv_row_count,
        "corpus_project_file_count": corpus_project_file_count,
        "excluded_from_scoring": excluded_from_scoring,
        "blocking_conditions": blocking_conditions,
        "warnings": warnings,
    }


# ── evidence map ──────────────────────────────────────────────────────────────

def _artifact(
    artifact_id, path, artifact_type, required, present, producer, authority_level,
    context_role, grain, key_fields, identifiers, join_keys, can_answer, cannot_answer,
    known_limitations, null_semantics, related_artifacts, schema_version=None,
):
    entry = {
        "artifact_id": artifact_id,
        "path": path,
        "artifact_type": artifact_type,
        "required": required,
        "present": present,
        "producer": producer,
        "authority_level": authority_level,
        "context_role": context_role,
        "grain": grain,
        "key_fields": key_fields,
        "identifiers": identifiers,
        "join_keys": join_keys,
        "can_answer": can_answer,
        "cannot_answer": cannot_answer,
        "known_limitations": known_limitations,
        "null_semantics": null_semantics,
        "related_artifacts": related_artifacts,
    }
    if schema_version is not None:
        entry["schema_version"] = schema_version
    return entry


_BLANK_STRING_NULL_SEMANTICS = {
    "*": (
        "Blank string ('') for not-applicable/not-computed numeric fields "
        "(e.g. single-view schema rows lack used_*/bundle_* columns); there "
        "is no explicit null marker in this CSV -- consumers must treat '' "
        "as missing, not 0."
    ),
}


def build_evidence_map(
    *,
    schema_version: str,
    input_paths: dict,          # artifact_id -> Optional[Path]
    input_present: dict,        # artifact_id -> bool
    output_paths: dict,         # artifact_id -> Path
    sibling_paths: dict,        # artifact_id -> Path (inferred, not CLI args)
    sibling_present: dict,      # artifact_id -> bool
    package_schema_version: str = PACKAGE_SCHEMA_VERSION,
    # The actual schema_version governance_package_manifest.json and
    # governance_package_health.json were written with -- i.e. the runtime
    # value (args.package_schema_version), not the module default. Those two
    # files may be written with an overridden --package-schema-version; their
    # evidence-map entries must declare the same value they actually contain,
    # not PACKAGE_SCHEMA_VERSION unconditionally.
) -> dict:
    artifacts = []

    def p(paths, key):
        v = paths.get(key)
        return str(v) if v else None

    artifacts.append(_artifact(
        "cross_segment_summary", p(input_paths, "cross_segment_summary"), "csv", True,
        input_present.get("cross_segment_summary", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "primary pairwise comparison evidence -- feeds build_cascade(), "
        "build_client_summary(), render_discipline_section()",
        "one row per (comparison_run_id, segment_id_a, segment_id_b, domain, "
        "comparison_type) directed pair",
        ["comparison_run_id", "segment_id_a", "segment_id_b", "domain", "comparison_type"],
        ["segment_id_a", "segment_id_b"],
        ["segment_id_a", "segment_id_b (join to build_segment_manifest.py's segment_id)"],
        ["containment/Jaccard between two segments for a domain",
         "which comparison_type bucket (directed cascade stage, sibling, within_project) a pair belongs to"],
        ["business_center_label/discipline_label/collection_label on the pooled "
         "(focal-vs-pool) side -- see cross_segment_pooled.csv for pool-relative reads"],
        ["comparison_type is not a closed enum -- see compare_cross_segment.py's "
         "DIRECTED_TYPES/GOVERNANCE_STATE_DIRECTED_TYPES/discover_* literal emissions. "
         "An unrecognized value is excluded from cascade scoring by build_cascade() "
         "and only surfaces via governance_package_health.json's comparison_type_coverage, "
         "not via this CSV itself."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_pooled", "governance_domain_summary", "governance_client_summary"],
    ))

    artifacts.append(_artifact(
        "cross_segment_pooled", p(input_paths, "cross_segment_pooled"), "csv", True,
        input_present.get("cross_segment_pooled", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "focal-vs-pool containment evidence for client/business-center rollups "
        "-- feeds build_client_summary() and gt_by_scope/gc_by_scope/gp_by_scope",
        "one row per (comparison_run_id, segment_id, pool_scope) -- a focal "
        "segment against a named pool",
        ["comparison_run_id", "segment_id", "pool_scope", "domain"],
        ["segment_id"], ["segment_id (join to build_segment_manifest.py's segment_id)"],
        ["how a focal segment's vocabulary relates to its named pool (parent_sibling/bc/client grain)"],
        ["discipline_label and collection_label are not columns on this file"],
        ["build_client_summary() reads client_label/n_files_focal from every "
         "pool_scope grain without filtering by pool_scope (see "
         "docs/governance_narrative_scope_gap_audit.md finding A2); safe today "
         "only because those two fields are pool-scope-invariant, not because "
         "pool_scope is checked at the read site."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_summary", "governance_client_summary"],
    ))

    artifacts.append(_artifact(
        "cross_segment_governance_states", p(input_paths, "cross_segment_governance_states"), "csv", False,
        input_present.get("cross_segment_governance_states", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "row-level provided/used/passive/missing/local-active classification, "
        "detail grain behind cross_segment_governance_state_summary.csv",
        "one row per comparison + governance-state classification",
        ["domain", "comparison_type", "state"], ["segment_id_a", "segment_id_b"], [],
        ["individual state transitions (provided_and_used, provided_but_passive, "
         "etc.) per pattern"],
        ["aggregate shares/rankings -- use cross_segment_governance_state_summary.csv for that"],
        ["if absent, build_governance_state_summary() falls back to whatever "
         "cross_segment_governance_state_summary.csv rows are available; if both "
         "are absent, render_header()'s state_note says provided/used/passive/"
         "missing/local signals are inferred only indirectly."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_governance_state_summary", "governance_domain_summary"],
    ))

    artifacts.append(_artifact(
        "cross_segment_governance_state_summary", p(input_paths, "cross_segment_governance_state_summary"), "csv", False,
        input_present.get("cross_segment_governance_state_summary", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "compact per-domain governance-state aggregate consumed by build_governance_state_summary()",
        "one row per (domain, comparison_type) compact aggregate",
        ["domain", "comparison_type"], [], [],
        ["provided_to_used/passive/missing shares and counts per domain"],
        ["row-level per-pattern detail -- use cross_segment_governance_states.csv for that"],
        ["if upstream rows are not deduplicated to unique patterns, count fields "
         "(provided_and_used_count etc.) should be read as comparison-state rows, "
         "not unique-pattern counts -- see render_limitations()'s state_note."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_governance_states", "governance_domain_summary"],
    ))

    artifacts.append(_artifact(
        "cross_segment_delta", p(input_paths, "cross_segment_delta"), "csv", False,
        input_present.get("cross_segment_delta", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "legacy delta-pattern summary, only consulted when "
        "cross_segment_governance_state_summary.csv is absent (main()'s "
        "`elif delta_summary:` branch)",
        "one row per legacy delta comparison", ["domain"], [], [],
        ["legacy provided/local drift signal when governance-state outputs are unavailable"],
        ["provided/used/passive/missing state breakdown -- superseded by governance-state outputs"],
        ["main()'s section assembly is `if governance_state_summary: ... elif "
         "delta_summary: ...` -- this file's section is never rendered when "
         "governance-state outputs are also supplied, even if both are passed on the CLI."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_governance_state_summary"],
    ))

    artifacts.append(_artifact(
        "file_metadata", p(input_paths, "file_metadata"), "csv", False,
        input_present.get("file_metadata", False), "fingerprint pipeline (file_metadata.csv export)",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "corpus composition (file counts by role/discipline/client) via load_corpus_counts()",
        "one row per Revit file", ["governance_role", "discipline_label", "client_label"],
        [], [],
        ["how many Template/Container/Project files exist, discipline/client vocabulary present"],
        ["per-domain comparison scores -- this file has no domain column"],
        ["if absent, corpus counts default to zero and disc/client lists render "
         "as 'Unknown' in render_header()."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["governance_narrative_context"],
    ))

    artifacts.append(_artifact(
        "client_sector", p(input_paths, "client_sector"), "csv", False,
        input_present.get("client_sector", False), "human-curated (policies/client_sector.csv default)",
        AUTHORITY_USER_PROVIDED_NOTE,
        "client_label -> sector classification driving cross-client convergence "
        "tiering (xc in build_cascade())",
        "one row per client_label", ["client_label", "sector"], ["client_label"], [],
        ["which clients share a sector for cross-client comparison purposes"],
        ["any other cascade stage -- sector affects only cross-client convergence grouping"],
        ["absent/missing file: every client is 'unknown' sector (main() warns to "
         "stderr, does not error); --client-sector has a non-empty default path "
         "(policies/client_sector.csv), so 'not passed on CLI' is not the same "
         "as 'absent' -- see governance_package_health.json's client_sector_status "
         "for the explicit/default/missing distinction. See "
         "docs/governance_narrative_scope_gap_audit.md finding C7."],
        {"*": "Missing client_label from this file simply means unclassified sector, not an error."},
        ["governance_client_summary"],
    ))

    artifacts.append(_artifact(
        "cross_segment_union_inventory", p(input_paths, "cross_segment_union_inventory"), "csv", False,
        input_present.get("cross_segment_union_inventory", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds render_union_reuse_summary() alongside reuse-distribution/matrix-manifest",
        "one row per union-inventory grain (view_scope/unit_system/domain)",
        ["domain", "view_scope", "unit_system"], [], [],
        ["pattern presence breadth across the corpus for the union/reuse narrative block"],
        [], ["the narrative section is entirely omitted (not blank-rendered) if "
             "this + reuse-distribution + matrix-manifest are all absent -- "
             "render_union_reuse_summary() returns None, not an empty string, "
             "in that case."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["pattern_reuse_distribution", "matrix_output_manifest"],
    ))

    artifacts.append(_artifact(
        "pattern_reuse_distribution", p(input_paths, "pattern_reuse_distribution"), "csv", False,
        input_present.get("pattern_reuse_distribution", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds render_union_reuse_summary()'s top-20 reuse bucket table by domain",
        "one row per reuse bucket", ["domain", "reuse_bucket"], [], [],
        ["pattern reuse concentration for the union/reuse narrative block"],
        [], ["render_union_reuse_summary() consumes this via a top-20 bucket table "
             "only; full distribution detail beyond that is not summarized."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_union_inventory", "matrix_output_manifest"],
    ))

    artifacts.append(_artifact(
        "matrix_output_manifest", p(input_paths, "matrix_output_manifest"), "csv", False,
        input_present.get("matrix_output_manifest", False), "compare_cross_segment.py",
        AUTHORITY_CONVENIENCE_SUMMARY,
        "metadata-only today -- not integrated into narrative content beyond "
        "descriptive bullets; see docs/governance_generator_cross_compare_coverage.md",
        "one row per matrix artifact (matrix_name)", ["matrix_name"], [], [],
        ["which project/portfolio matrices exist and their documented interpretation/known_limitations text"],
        ["no narrative claims currently derive from this file's field content -- "
         "only its presence/absence and matrix_name are used"],
        ["no structured block/status column exists on this file today "
         "(MATRIX_MANIFEST_FIELDS has no status/blocked field) -- see "
         "governance_package_health.json's matrix_manifest.note."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_union_inventory", "pattern_reuse_distribution"],
    ))

    artifacts.append(_artifact(
        "cross_segment_file_pairs", p(sibling_paths, "file_pairs"), "csv", False,
        sibling_present.get("file_pairs", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "archive_only -- not read by generate_governance_narrative.py; reserved "
        "for drill-through/audit-pack use per "
        "docs/governance_generator_cross_compare_coverage.md ('too large for "
        "leadership summary, but the best evidence trail when a tier or anomaly "
        "needs file-level audit')",
        "one row per file pair", ["segment_id_a", "segment_id_b", "domain"], [], [],
        ["file-level audit trail behind any pair-mean metric in cross_segment_summary.csv"],
        ["this generator does not open or parse this file; presence is inferred "
         "as a sibling of --summary's directory, never verified against its own schema"],
        ["not consumed by this generator in PR1; see "
         "docs/governance_generator_cross_compare_coverage.md's suggested "
         "'drill-through only' integration point"],
        {},
        ["cross_segment_summary"],
    ))

    artifacts.append(_artifact(
        "comparison_registry", p(sibling_paths, "comparison_registry"), "csv", False,
        sibling_present.get("comparison_registry", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "archive_only -- not read by generate_governance_narrative.py; tracks "
        "comparison staleness/forced-rerun state per "
        "docs/governance_generator_cross_compare_coverage.md's recommended "
        "'Input Completeness / Staleness' integration point",
        "one row per (domain, segment pair) comparison registry entry", ["domain"], [], [],
        ["whether an expected segment/domain comparison was actually run, and whether it is stale"],
        ["this generator does not open or parse this file; presence is inferred "
         "as a sibling of --summary's directory, never verified against its own schema"],
        ["not consumed by this generator in PR1 -- missing rows in "
         "cross_segment_summary.csv are currently treated as weak evidence rather "
         "than distinguished from not-run/stale comparisons; see "
         "docs/governance_generator_cross_compare_coverage.md."],
        {},
        ["cross_segment_summary"],
    ))

    artifacts.append(_artifact(
        "governance_domain_summary", p(output_paths, "governance_domain_summary"), "csv", True, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "primary tier/score rollup, one row per domain",
        "one row per domain with a renderable cascade signal (domains failing "
        "_has_renderable_cascade_signal() -- Group-3-scope-only domains -- are excluded)",
        ["domain", "governance_tier", "template_to_project"], ["domain"], [],
        ["tier/reliability/anomaly classification and cascade scores per domain"],
        ["client-level or discipline-level breakdowns -- see governance_client_summary.csv "
         "and the narrative's discipline section"],
        ["excludes EXCLUDED_FROM_SCORING domains from aggregate framing (still "
         "listed as a row); fmt()/pct() render a present-but-None numeric field "
         "as the em-dash — string (not an ASCII hyphen), while a governance-state "
         "column for a domain with no governance_state_summary entry at all "
         "renders as '' (empty string) -- two different 'missing' states use "
         "two different cell conventions in this CSV, documented but not "
         "unified in PR1."],
        {
            "*(fmt/pct-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists in the schema but has no data for this domain.",
            "*(governance-state columns)": "'' (empty string) means governance_state_summary has no entry for this domain at all -- a different condition than a present-but-None value.",
        },
        ["cross_segment_summary", "cross_segment_pooled", "cross_segment_governance_state_summary"],
    ))

    artifacts.append(_artifact(
        "governance_client_summary", p(output_paths, "governance_client_summary"), "csv", True, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "primary client alignment/onboarding rollup, one row per client",
        "one row per client with at least one Project file discovered via "
        "pooled_rows/summary_rows",
        ["client", "alignment_tier"], ["client"], [],
        ["cross-client similarity and within-project coherence per client, plus "
         "onboarding-oriented interpretation fields"],
        ["per-domain detail -- see governance_domain_summary.csv"],
        ["inherits the cross_segment_pooled.csv A2 pool_scope caveat -- see that "
         "artifact's known_limitations."],
        {"*(fmt-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists but has no data for this client."},
        ["cross_segment_summary", "cross_segment_pooled", "client_sector"],
    ))

    artifacts.append(_artifact(
        "governance_narrative_context", p(output_paths, "governance_narrative_context"), "markdown", True, True,
        GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
        "human-readable synthesis; sections list assembled from render_* functions",
        "one markdown document per run", [], [], [],
        ["a human-readable synthesis of the two CSVs above, with tier labels and framing prose"],
        ["approves no standard, assigns no owner, judges no team -- this is the "
         "generator's own stated scope boundary (render_header()'s Executive Summary)"],
        ["assembled by conditional section inclusion -- governance-state and "
         "delta sections are mutually exclusive (elif); the union/reuse section "
         "is entirely omitted, not blank-rendered, when all three of its inputs "
         "are absent."],
        {},
        ["governance_domain_summary", "governance_client_summary",
         "governance_package_health", "governance_evidence_map", "governance_findings",
         "governance_brief", "governance_interpretation_guide", "governance_question_routes"],
    ))

    artifacts.append(_artifact(
        "governance_findings", p(output_paths, "governance_findings"), "json", False, True,
        GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
        "structured, rule-derived findings (tier/anomaly/onboarding classifications) "
        "with provenance -- origin, fidelity, authority, and limits per finding, "
        "plus leadership questions marked as questions rather than claims",
        "one object per package generation run, containing one entry per finding",
        [], ["finding_id"], [],
        ["which domains/clients meet a specific named governance rule "
         "(baseline_candidate, high_fragmentation, passive_inheritance_risk, etc.), "
         "and what CSV fields/rows support that classification"],
        ["raw metric values -- follow each finding's support[].selector back to "
         "governance_domain_summary.csv/governance_client_summary.csv for those"],
        ["derived by build_structured_findings(), which reuses the exact same "
         "classification buckets governance_narrative_context.md's Key Findings "
         "section renders as prose -- the two are not independent implementations. "
         "leadership_question findings carry status: question_not_claim and "
         "authority_level: convenience_summary -- they are suggested questions, "
         "not observed results."],
        {},
        ["governance_domain_summary", "governance_client_summary", "governance_narrative_context"],
        schema_version=FINDINGS_SCHEMA_VERSION,
    ))

    # governance_brief.md is the only PR4 artifact that may genuinely be
    # absent even when this whole function runs (gated by its own
    # --emit-interpretation-layer flag, independent of --emit-evidence-package)
    # -- unlike the artifacts above, whose "present: True" is hardcoded because
    # build_evidence_map() only ever runs after they're already written.
    _brief_path = output_paths.get("governance_brief")
    _brief_present = bool(_brief_path) and Path(_brief_path).exists()
    artifacts.append(_artifact(
        "governance_brief", p(output_paths, "governance_brief") if _brief_present else None,
        "markdown", False, _brief_present,
        GENERATOR_IDENTITY, AUTHORITY_CONVENIENCE_SUMMARY,
        "narrower, run-specific digest of governance_findings.json -- a quick "
        "top-line read, not a new source of evidence",
        "one markdown document per run (when --emit-interpretation-layer is on)",
        [], [], [],
        ["a capped, categorized list of this run's findings by finding_type "
         "(baseline candidates, high fragmentation, passive-inheritance risk, "
         "low client coherence), plus the leadership questions"],
        ["anything beyond what governance_findings.json already contains -- "
         "this is a distillation, computed from the same findings list, never "
         "an independent computation"],
        ["each finding-type section is capped (10-15 items) with a pointer to "
         "governance_findings.json for the full list; absent entirely when "
         "--no-emit-interpretation-layer was passed for this run -- check this "
         "artifact's own present field, not just governance_package_manifest's "
         "policy_profiles, to know whether it exists for a given run."],
        {},
        ["governance_findings", "governance_domain_summary", "governance_client_summary",
         "governance_interpretation_guide", "governance_question_routes"],
    ))

    artifacts.append(_artifact(
        "governance_interpretation_guide", p(sibling_paths, "interpretation_guide"),
        "markdown", False, sibling_present.get("interpretation_guide", False),
        "human/LLM-authored (docs/governance_interpretation_guide.md)",
        AUTHORITY_CONTROLLED_INTERPRETATION,
        "package-specific interpretation layer: what each metric/tier means, "
        "comparability rules, missing-value semantics, authority ordering, "
        "known bad inferences -- read this before reasoning from the rest of "
        "the package",
        "one document per package_type (not per-run; not regenerated by this "
        "generator)",
        [], [], [],
        ["what a metric or governance_tier value means and does not mean; how "
         "to read missing values and comparability caveats for this package type"],
        ["this run's actual data -- it explains semantics, not this run's results"],
        ["not written or validated by this generator; presence is a real "
         "Path.exists() check against the checked-in repo doc, not a per-run "
         "guarantee -- a package copied without the repo's docs/ directory "
         "would show present: false here"],
        {},
        ["governance_question_routes", "governance_brief", "governance_narrative_context"],
    ))

    artifacts.append(_artifact(
        "governance_question_routes", p(sibling_paths, "question_routes"),
        "markdown", False, sibling_present.get("question_routes", False),
        "human/LLM-authored discovery scaffold (docs/governance_question_routes.md)",
        AUTHORITY_CONVENIENCE_SUMMARY,
        "candidate catalog of recurring question types and which artifact/"
        "fields answer them -- navigational only, not evidence",
        "one document per package_type (not per-run; not regenerated by this "
        "generator)",
        [], [], [],
        ["which artifact to check first for a specific recurring question type"],
        ["the answer itself -- follow the route to the named artifact"],
        ["every route in this document is at 'candidate' maturity (see the "
         "document's own maturity-level scale) -- none has a proven history "
         "of repeated use for this package type yet; not an exhaustive list "
         "of every possible question"],
        {},
        ["governance_interpretation_guide", "governance_brief", "governance_findings"],
    ))

    artifacts.append(_artifact(
        "governance_package_manifest", p(output_paths, "governance_package_manifest"), "json", False, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "provenance record: which inputs were provided/found, which outputs "
        "were written and their sizes, comparison_run_id(s)/executed_utc "
        "observed in the loaded rows",
        "one object per package generation run", [], [], [],
        ["was this run reproducible-in-principle from a known input set; which "
         "optional inputs were actually supplied"],
        ["input CSV content correctness -- only presence/path/size is validated, "
         "never parsed content"],
        [], {}, ["governance_package_health", "governance_evidence_map", "governance_findings"],
        schema_version=package_schema_version,
    ))

    artifacts.append(_artifact(
        "governance_package_health", p(output_paths, "governance_package_health"), "json", False, True,
        GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
        "aggregated data-quality/coverage signal: schema detection, used-view "
        "fallback, comparison_type coverage, which optional CSVs were present",
        "one object per package generation run", [], [], [],
        ["should this narrative's used-view claims be trusted at face value; "
         "were any comparison_type values excluded from cascade scoring"],
        ["does not repeat or replace governance_narrative_context.md's own "
         "Analytical Notes and Limitations section -- this is a machine-readable "
         "companion, not a superseding source"],
        [], {}, ["governance_package_manifest", "governance_evidence_map", "governance_findings"],
        schema_version=package_schema_version,
    ))

    artifacts.append(_artifact(
        "governance_evidence_map", p(output_paths, "governance_evidence_map"), "json", False, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "index of every artifact in the package, this file included, so a "
        "downstream consumer can discover what exists and what it can/cannot "
        "answer without re-deriving it",
        "one object per package, containing one entry per artifact", [], [], [],
        ["what artifacts exist in this package and where to look for a given question type"],
        ["makes no claim about the narrative's own authority -- see the "
         "governance_narrative_context artifact entry's authority_level"],
        [], {},
        [],  # filled in below, once every other artifact_id is known
        schema_version=EVIDENCE_MAP_SCHEMA_VERSION,
    ))

    all_ids = [a["artifact_id"] for a in artifacts]
    for a in artifacts:
        if a["artifact_id"] == "governance_evidence_map":
            a["related_artifacts"] = [aid for aid in all_ids if aid != "governance_evidence_map"]

    return {"schema_version": schema_version, "artifacts": artifacts}
