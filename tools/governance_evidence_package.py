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

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── package identity / schema versions ──────────────────────────────────────

PACKAGE_TYPE = "governance_evidence_package"
PACKAGE_SCHEMA_VERSION = "1.0"
EVIDENCE_MAP_SCHEMA_VERSION = "1.1"
FINDINGS_SCHEMA_VERSION = "1.0"
FILE_INVENTORY_SCHEMA_VERSION = "1.0"

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
    known_limitations, null_semantics, related_artifacts, *, required_before_conclusions,
    schema_version=None,
):
    # required_before_conclusions is keyword-only with no default (D-030): every
    # call site must state explicitly whether a governance conclusion drawn
    # without this artifact would be unsafe, so a future artifact addition can't
    # silently inherit a wrong default -- see build_evidence_map()'s
    # reasoning_prerequisites and docs/governance_reading_order.md.
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
        "required_before_conclusions": required_before_conclusions,
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


def _sibling_scan_fields(path, present: bool) -> dict:
    """Reuse _scan_csv_file() -- the same D-023 live scan governance_file_
    inventory.json already performs for undiscovered files -- to populate an
    excluded sibling artifact's own governance_evidence_map.json entry with
    its column header (name + inferred dtype) and row count. A reader who
    never opens governance_file_inventory.json still gets this for the
    specific large files docs/governance_interpretation_guide.md's
    escalation section names by filename (D-024). Returns {} when the file
    is not present -- scanning a path that does not exist is meaningless,
    not an error to report. Never returns a sample row or cell value, same
    scope decision as the D-023 scan itself.
    """
    if not present or not path:
        return {}
    scan = _scan_csv_file(Path(path))
    fields = {"row_count": scan["row_count"]}
    if scan.get("parse_error"):
        fields["parse_error"] = scan["parse_error"]
    else:
        fields["columns"] = scan["columns"]
    return fields


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
    file_inventory_schema_version: str = FILE_INVENTORY_SCHEMA_VERSION,
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
        required_before_conclusions=True,
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
        required_before_conclusions=True,
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
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "segment_manifest", p(input_paths, "segment_manifest"), "csv", False,
        input_present.get("segment_manifest", False), "build_segment_manifest.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "lets build_cascade()'s within_project score_reliability p10/p90 capture "
        "resolve a redundant_single_child-demoted enterprise-wide root segment to "
        "its population-identical runnable descendant via _resolve_runnable_segment() "
        "(imported from compare_cross_segment.py), instead of finding no unscoped "
        "segment at all",
        "one row per segment_id in the full segmentation lattice",
        ["segment_id", "run_type"], ["segment_id"],
        ["segment_id (join to cross_segment_summary.csv's segment_id_a/_b)"],
        ["whether a segment_id is directly runnable (run_type in bundle/reference) "
         "or redundant_single_child to a population-identical descendant"],
        ["per-domain comparison scores -- this file has no domain column"],
        ["absent: within_project score_reliability p10/p90 only ever populate from "
         "a row that is directly _is_unscoped_segment() -- the pre-existing, "
         "narrower behavior -- rather than also resolving a demoted root through "
         "its redundant_single_child chain."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["cross_segment_summary", "governance_domain_summary"],
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        ["cross_segment_union_inventory", "matrix_output_manifest", "pattern_reuse_summary_by_domain"],
        required_before_conclusions=False,
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
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "pattern_reuse_summary_by_client", p(input_paths, "pattern_reuse_summary_by_client"), "csv", False,
        input_present.get("pattern_reuse_summary_by_client", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds render_union_reuse_summary()'s adoption-breadth cut (how many "
        "clients reach a corpus-wide-reused pattern per domain) -- additive to, "
        "and independent of, the distinct-pattern reuse table sourced from "
        "pattern_reuse_distribution.csv",
        "one row per (view_scope, governance_role, client_label, "
        "discipline_label, unit_system, domain, reuse_bucket, bucket_basis) "
        "-- n_patterns is a bucket_basis-scoped occurrence count, not a "
        "distinct-pattern count",
        ["domain", "client_label", "reuse_bucket"], [], [],
        ["how many of a domain's clients have at least one corpus-wide-reused pattern"],
        ["distinct-pattern counts across the whole corpus -- use "
         "pattern_reuse_distribution.csv for that; this file is grouped by "
         "client_label so the same pattern is counted once per client, not once total"],
        ["pattern_reuse_summary_by_domain.csv (the by-domain sibling of this file) "
         "is deliberately not consumed -- its n_patterns duplicates the "
         "corpus-wide reuse signal the distinct-pattern table already reports."],
        _BLANK_STRING_NULL_SEMANTICS,
        ["pattern_reuse_distribution", "cross_segment_union_inventory"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "project_union_jaccard_matrix", p(input_paths, "project_union_jaccard_matrix"), "csv", False,
        input_present.get("project_union_jaccard_matrix", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Project Portfolio section's footprint-identity paragraph "
        "(render_project_portfolio_section())",
        "one row per (row_id, column_id, view_scope, domain) matrix cell; "
        "ALL_DOMAINS rows carry the system-level union_jaccard used in the narrative",
        ["row_id", "column_id", "view_scope", "domain"], [], [],
        ["whether two projects' systems contain the same canonical patterns "
         "(exact footprint overlap), independent of file-pair identity"],
        ["typical file-to-file similarity -- use project_mean_file_pair_jaccard_matrix.csv's "
         "signal, folded into project_fragmentation_diagnostic.csv, for that"],
        ["symmetric matrix -- both (a, b) and (b, a) rows are emitted; the "
         "narrative dedupes to one row per unordered project pair"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["project_density_similarity_matrix", "project_fragmentation_diagnostic", "matrix_output_manifest"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "project_density_similarity_matrix", p(input_paths, "project_density_similarity_matrix"), "csv", False,
        input_present.get("project_density_similarity_matrix", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Project Portfolio section's density-similarity paragraph, "
        "cross-referenced against project_union_jaccard_matrix.csv for the "
        "\"same shape, different content\" caveat when supplied",
        "one row per (row_id, column_id, view_scope, domain) matrix cell; "
        "ALL_DOMAINS rows carry the system-level density_similarity used in the narrative",
        ["row_id", "column_id", "view_scope", "domain"], [], [],
        ["whether two projects populate the same domains to a similar degree "
         "(occupancy-count cosine similarity), independent of exact pattern identity"],
        ["exact pattern identity -- high density similarity with low "
         "union_jaccard means same shape, different content, not the same content"],
        ["symmetric matrix, same dedup treatment as project_union_jaccard_matrix.csv; "
         "the same-shape/different-content cross-check is unavailable when "
         "project_union_jaccard_matrix.csv is not also supplied"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["project_union_jaccard_matrix", "matrix_output_manifest"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "project_pool_containment_similarity_matrix", p(input_paths, "project_pool_containment_similarity_matrix"), "csv", False,
        input_present.get("project_pool_containment_similarity_matrix", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Project Portfolio section's peer-pool-containment paragraph, "
        "rendered as a per-project outlier list (not a per-pair table)",
        "one row per (row_id=focal_project, column_id=peer_pool:{pool_scope}:{row_id}, "
        "view_scope, domain) -- unlike the other three project matrices, this "
        "one carries no ALL_DOMAINS aggregate row",
        ["row_id", "column_id", "view_scope", "domain"], [], [],
        ["how much a project's system aligns with its parent-sibling/bc/client peer pool"],
        ["a cross-domain aggregate straight from this file -- the narrative "
         "computes its own mean pool_containment_similarity across a project's "
         "available domains per (project, pool_scope) because no ALL_DOMAINS "
         "row exists here"],
        ["column_id encodes pool_scope (parent_sibling/bc/client) so a "
         "project's separate pool grains never share a matrix cell"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["matrix_output_manifest"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "project_fragmentation_diagnostic", p(input_paths, "project_fragmentation_diagnostic"), "csv", False,
        input_present.get("project_fragmentation_diagnostic", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Project Portfolio section's fragmentation-diagnostic "
        "paragraph; also the sole carrier of project_mean_file_pair_jaccard_matrix.csv's "
        "signal in this narrative (its own exact_identity_overlap column), rather "
        "than that matrix being consumed standalone",
        "one row per (row_id, column_id, view_scope, domain=ALL_DOMAINS) -- "
        "footprint_similarity minus exact_identity_overlap when both inputs "
        "were available at production time",
        ["row_id", "column_id", "view_scope"], [], [],
        ["divergence between project footprint overlap (union_jaccard) and "
         "exact per-file identity overlap (mean file-pair jaccard)"],
        ["an authoritative governance index -- diagnostic only, per this "
         "file's own interpretation text"],
        ["value_status other than \"diagnostic\" (e.g. unavailable_required_inputs) "
         "means the cell could not be computed and is excluded from the "
         "narrative's pair list"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["project_union_jaccard_matrix", "matrix_output_manifest", "project_mean_file_pair_jaccard_matrix"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "governance_bc_client_matrix", p(input_paths, "governance_bc_client_matrix"), "csv", False,
        input_present.get("governance_bc_client_matrix", False), "tools/governance_relationships.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Business Center Composition section -- client composition "
        "of each business center's PHYSICAL project population (file_metadata.csv's "
        "project_label grain), not the governance-population grain used by the "
        "project_* matrices above",
        "one row per (business_center_label, client_label) pair actually present",
        ["business_center_label", "client_label"], [], [],
        ["how many physical projects/files a client contributes to a business "
         "center's population, and what share of that business center's files "
         "that represents (percentage_of_bc, computed exactly once in "
         "build_bc_client_matrix_rows() and only read here)"],
        ["behavioral similarity between those projects -- see "
         "project_pool_containment_similarity_matrix.csv for that, and note its "
         "\"project\" grain is a (client, discipline, unit_system) governance "
         "population, not the same entity as a row here; the two are not "
         "row-for-row joinable"],
        ["percentage_of_client on this file answers a different question than "
         "percentage_of_bc on the same row -- one BC's share of one client's "
         "total files vs. one client's share of one BC's total files; do not "
         "average or compare them directly"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["governance_client_bc_matrix", "governance_relationships"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "governance_relationships", p(sibling_paths, "governance_relationships"), "csv", False,
        sibling_present.get("governance_relationships", False), "tools/governance_relationships.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "archive_only -- not read by generate_governance_narrative.py; the "
        "one-row-per-physical-project source that governance_bc_client_matrix.csv/"
        "governance_client_bc_matrix.csv are aggregated from, named by path in "
        "the Business Center Composition section's body text",
        "one row per (client_label, business_center_label, project_name) "
        "physical-project identity",
        ["client_label", "business_center_label", "project_name"], ["project_id"], [],
        ["which physical projects exist for a client/business center, and how "
         "many files each carries; whether a project_name string is genuinely "
         "one project or a same-named collision across different clients"],
        ["a governance, compliance, or quality read -- project/file counts only"],
        ["project_name_is_fallback == \"true\" means project_name is a synthetic "
         "per-file identifier (that file's own export_run_id), not a human-"
         "assigned project name -- not consumed or checked by this generator, "
         "which only infers this file's presence beside whichever of "
         "--governance-bc-client-matrix/--governance-client-bc-matrix was "
         "supplied (falling back to --summary's directory if neither was) "
         "and never parses it"],
        {},
        ["governance_bc_client_matrix", "governance_client_bc_matrix"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "governance_client_bc_matrix", p(input_paths, "governance_client_bc_matrix"), "csv", False,
        input_present.get("governance_client_bc_matrix", False), "tools/governance_relationships.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "feeds the Business Center Distribution section -- business-center "
        "distribution of each client's physical project population, aggregated "
        "from governance_bc_client_matrix.csv with no independent computation",
        "one row per client_label",
        ["client_label"], [], [],
        ["how many business centers a client's projects span, and how that "
         "client's project/file count divides across them (business_centers is "
         "already ordered by governance_bc_client_matrix.csv's percentage_of_client, "
         "descending)"],
        ["a percentage_of_bc/percentage_of_client column of its own -- this file "
         "only sums project_count/project_file_count from governance_bc_client_"
         "matrix.csv; read percentages from that file, not this one"],
        ["in the corpus this package type was seeded from, no client's projects "
         "actually spanned more than one business center (business_center_count "
         "== 1 for every row) -- a single-BC client here is a real, verified-"
         "common case, not evidence the multi-BC aggregation path is untested "
         "(see tests/test_governance_relationships.py's synthetic multi-BC case)"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["governance_bc_client_matrix"],
        required_before_conclusions=False,
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
         "'drill-through only' integration point. columns/row_count below "
         "(when present) come from the same live directory scan governance_"
         "file_inventory.json uses (_scan_csv_file, D-023/D-024) -- a "
         "structural fact about the header, not this generator opening or "
         "interpreting a single row of it."],
        {},
        ["cross_segment_summary"],
        required_before_conclusions=False,
    ))
    artifacts[-1].update(_sibling_scan_fields(sibling_paths.get("file_pairs"), sibling_present.get("file_pairs", False)))

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
         "docs/governance_generator_cross_compare_coverage.md. columns/row_count "
         "below (when present) come from the same live directory scan governance_"
         "file_inventory.json uses (_scan_csv_file, D-023/D-024), not from a "
         "read this generator performs on a normal run."],
        {},
        ["cross_segment_summary"],
        required_before_conclusions=False,
    ))
    artifacts[-1].update(_sibling_scan_fields(
        sibling_paths.get("comparison_registry"), sibling_present.get("comparison_registry", False),
    ))

    artifacts.append(_artifact(
        "pattern_reuse_summary_by_domain", p(sibling_paths, "pattern_reuse_summary_by_domain"), "csv", False,
        sibling_present.get("pattern_reuse_summary_by_domain", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "archive_only -- not read by generate_governance_narrative.py; "
        "deliberately excluded (not merely unwired), since its n_patterns "
        "duplicates the corpus-wide reuse signal pattern_reuse_distribution.csv's "
        "own distinct-pattern table already reports -- see this generator's "
        "own module docstring and docs/governance_generator_cross_compare_coverage.md",
        "one row per (view_scope, governance_role, client_label, "
        "discipline_label, unit_system, domain, reuse_bucket, bucket_basis) "
        "-- the by-domain sibling of pattern_reuse_summary_by_client.csv",
        ["view_scope", "governance_role", "client_label", "discipline_label", "unit_system", "domain"], [], [],
        ["per-domain reuse_bucket/n_patterns counts, recorded independently of "
         "pattern_reuse_distribution.csv's own dedup table"],
        ["a governance signal distinct from what pattern_reuse_distribution.csv "
         "already reports -- evaluated and confirmed to add no new information "
         "beyond that file's already-consumed distinct-pattern table"],
        ["this generator's narrative/scoring logic never opens or interprets "
         "this file's row content; columns/row_count below (when present) come "
         "from the same live directory scan governance_file_inventory.json "
         "uses (_scan_csv_file, D-023/D-024), not from a read this generator "
         "performs on a normal run"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["pattern_reuse_distribution", "pattern_reuse_summary_by_client"],
        required_before_conclusions=False,
    ))
    artifacts[-1].update(_sibling_scan_fields(
        sibling_paths.get("pattern_reuse_summary_by_domain"),
        sibling_present.get("pattern_reuse_summary_by_domain", False),
    ))

    artifacts.append(_artifact(
        "project_mean_file_pair_jaccard_matrix", p(sibling_paths, "project_mean_file_pair_jaccard_matrix"), "csv", False,
        sibling_present.get("project_mean_file_pair_jaccard_matrix", False), "compare_cross_segment.py",
        AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "archive_only -- not consumed standalone by generate_governance_narrative.py; "
        "its signal is folded into project_fragmentation_diagnostic.csv's own "
        "exact_identity_overlap column instead, per this generator's own module "
        "docstring and docs/governance_generator_cross_compare_coverage.md",
        "one row per (row_id, column_id, view_scope, domain) matrix cell, same "
        "shape as the other project_* matrices; ALL_DOMAINS rows carry the "
        "cross-domain mean file-pair jaccard",
        ["row_id", "column_id", "view_scope", "domain"], [], [],
        ["typical file-to-file similarity between two projects (mean pairwise "
         "file jaccard), independent of exact system-level footprint overlap"],
        ["a governance read distinct from project_fragmentation_diagnostic.csv's "
         "exact_identity_overlap column -- that column already carries this "
         "file's signal into the narrative; this file itself is never opened "
         "standalone"],
        ["this generator's narrative/scoring logic never opens or interprets "
         "this file's row content directly; columns/row_count below (when "
         "present) come from the same live directory scan governance_file_"
         "inventory.json uses (_scan_csv_file, D-023/D-024), not from a read "
         "this generator performs on a normal run"],
        _BLANK_STRING_NULL_SEMANTICS,
        ["project_fragmentation_diagnostic", "project_union_jaccard_matrix"],
        required_before_conclusions=False,
    ))
    artifacts[-1].update(_sibling_scan_fields(
        sibling_paths.get("project_mean_file_pair_jaccard_matrix"),
        sibling_present.get("project_mean_file_pair_jaccard_matrix", False),
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
        required_before_conclusions=True,
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
        required_before_conclusions=True,
    ))

    artifacts.append(_artifact(
        "governance_bc_summary", p(output_paths, "governance_bc_summary"), "csv", True, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "primary business-center peer-alignment rollup, one row per business center",
        "one row per real business center discovered via bc_to_bc/enterprise_to_bc/"
        "bc_to_project summary rows or cascade's tc_bc_by_bc/eb_by_bc breakouts "
        "(build_bc_summary()) -- Enterprise itself is never a row here, see "
        "governance_narrative_context.md's Enterprise Overview section instead",
        ["business_center", "alignment_tier"], ["business_center"], [],
        ["cross-BC peer similarity (all-view primary -- opposite convention from "
         "governance_client_summary.csv's used-view-primary cross-client similarity, "
         "since bc_to_bc pairs are Template/Container peer comparisons, not Project "
         "usage comparisons), internal Template->Container coherence per BC, and "
         "Enterprise standard reach into that BC"],
        ["per-domain detail beyond the top/bottom-3 most/least-aligned columns -- "
         "see governance_domain_summary.csv; Enterprise's own rollup -- see the "
         "narrative's Enterprise Overview section, not this CSV"],
        ["bc_alignment_high/_moderate and bc_confidence_low/moderate_max_files "
         "thresholds are hand-picked defaults value-coincident with (but a "
         "separate policy profile from) governance_client_summary.csv's "
         "client_alignment_*/client_confidence_* thresholds -- see "
         "BC_ALIGNMENT_HIGH's definition comment in generate_governance_narrative.py."],
        {"*(fmt-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists but has no data for this business center."},
        ["cross_segment_summary", "governance_domain_summary", "governance_narrative_context"],
        required_before_conclusions=True,
    ))

    artifacts.append(_artifact(
        "governance_narrative_context", p(output_paths, "governance_narrative_context"), "markdown", True, True,
        GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
        "human-readable synthesis; sections list assembled from render_* functions",
        "one markdown document per run", [], [], [],
        ["a human-readable synthesis of the three CSVs above, with tier labels and framing prose"],
        ["approves no standard, assigns no owner, judges no team -- this is the "
         "generator's own stated scope boundary (render_header()'s Executive Summary)"],
        ["assembled by conditional section inclusion -- governance-state and "
         "delta sections are mutually exclusive (elif); the union/reuse section "
         "is entirely omitted, not blank-rendered, when all three of its inputs "
         "are absent; the Enterprise Overview section is likewise omitted (not "
         "blank-rendered) when cascade has no tc/eb/ec signal at all."],
        {},
        ["governance_domain_summary", "governance_client_summary", "governance_bc_summary",
         "governance_package_health", "governance_evidence_map", "governance_findings",
         "governance_brief", "governance_interpretation_guide", "governance_question_routes",
         "governance_reading_order"],
        required_before_conclusions=False,
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
        required_before_conclusions=True,
    ))

    artifacts.append(_artifact(
        "governance_file_inventory", p(output_paths, "governance_file_inventory"), "json", False, True,
        GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
        "live directory-scan inventory of *.csv files actually present under "
        "the cross_segment export directory (and, when supplied separately, "
        "the relationship-layer output directory) that are NOT already one of "
        "the artifacts above -- see inventory_export_directory_files(). Exists "
        "so an LLM reading this package can name a candidate drill-down file "
        "it has never been told the schema of, instead of stonewalling on a "
        "question the rollups can't answer",
        "one entry per undiscovered CSV file found during this run's scan; a "
        "corpus with no such files produces an entry with an empty files list, "
        "not a missing artifact",
        ["filename", "row_count"], [], [],
        ["that a given file exists in the scanned directories, its column "
         "header, an inferred dtype per column (integer/float/boolean/string/"
         "empty), and its row count"],
        ["what any column or row actually means -- the per-file narrative "
         "string is either borrowed verbatim from matrix_output_manifest.csv's "
         "own interpretation column when the filename matches a known "
         "matrix_name, or a generic structural fallback sentence; neither is a "
         "substitute for a real evidence-map entry once a file is understood "
         "well enough to earn one"],
        ["computed fresh every run from Path.glob('*.csv') -- a file deleted "
         "or renamed between runs simply stops/starts appearing, with no "
         "staleness tracking of its own; no sample cell values are ever "
         "captured, only header names, inferred dtype, and row count; column "
         "dtype inference is a best-effort classification over the whole "
         "column's values, not a schema declaration -- see _column_dtype()."],
        {"*": "A column classified 'empty' had zero non-blank cells in the scanned file."},
        [],  # no fixed related_artifacts -- the files it lists vary run to run
        schema_version=file_inventory_schema_version,
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        ["governance_question_routes", "governance_brief", "governance_narrative_context",
         "governance_reading_order"],
        required_before_conclusions=False,
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
        ["governance_interpretation_guide", "governance_brief", "governance_findings",
         "governance_reading_order"],
        required_before_conclusions=False,
    ))

    artifacts.append(_artifact(
        "governance_reading_order", p(sibling_paths, "reading_order"),
        "markdown", False, sibling_present.get("reading_order", False),
        "human/LLM-authored (docs/governance_reading_order.md)",
        AUTHORITY_CONTROLLED_INTERPRETATION,
        "cold-start reading sequence for this package (D-030): audience/"
        "purpose statement, an ordered path through the package, and a "
        "'read this before drawing conclusions' callout naming the D-031 "
        "known-bad-inference entries -- the human-readable counterpart to "
        "this file's own reasoning_prerequisites list",
        "one document per package_type (not per-run; not regenerated by this "
        "generator)",
        [], [], [],
        ["what order to read this package's artifacts in, and which two "
         "known-bad-inference entries to check before drawing a conclusion"],
        ["this run's actual data -- it is a fixed reading sequence, not a "
         "per-run result; it does not itself enumerate reasoning_prerequisites, "
         "see governance_evidence_map.json for the machine-checkable list"],
        ["not written or validated by this generator; presence is a real "
         "Path.exists() check against the checked-in repo doc, not a per-run "
         "guarantee -- a package copied without the repo's docs/ directory "
         "would show present: false here"],
        {},
        ["governance_interpretation_guide", "governance_question_routes",
         "governance_narrative_context", "governance_evidence_map"],
        required_before_conclusions=False,
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
        required_before_conclusions=False,
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
        required_before_conclusions=True,
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
        required_before_conclusions=False,
    ))

    all_ids = [a["artifact_id"] for a in artifacts]
    for a in artifacts:
        if a["artifact_id"] == "governance_evidence_map":
            a["related_artifacts"] = [aid for aid in all_ids if aid != "governance_evidence_map"]

    # D-030: the full set of required_before_conclusions=true artifact_ids,
    # exposed once at the manifest level -- a set to exhaust, not a sequence
    # to sample from. See docs/governance_reading_order.md.
    reasoning_prerequisites = [
        a["artifact_id"] for a in artifacts if a["required_before_conclusions"]
    ]

    return {
        "schema_version": schema_version,
        "artifacts": artifacts,
        "reasoning_prerequisites": reasoning_prerequisites,
    }


# ── file inventory (live directory scan) ─────────────────────────────────────
# Step 0 for this feature confirmed: (a) no query/tool-calling path exists
# anywhere in this package -- generate_governance_narrative.py's outputs are
# consumed single-shot, so an LLM reader can only know a drill-down file
# exists if this package says so; (b) no prior "csv_inventory.md"-style
# utility exists in this repo. The functions below are the mechanical
# directory-scan/schema-inference layer that fills that gap -- they read
# column headers and infer per-column dtype from the data, but never retain
# or report sample values (inventory, not analysis).

def _classify_scalar(value: str) -> str:
    """Classify one non-blank cell value as 'bool' / 'int' / 'float' / 'string'.

    Matches this codebase's own CSV-writing conventions: compare_cross_segment.py's
    _bool_str() emits exactly "true"/"false" (see its own definition) for boolean
    fields, never "True"/"1"/"yes" -- so bool detection is intentionally narrow
    (case-insensitive true/false only) rather than guessing at every truthy-looking
    token.
    """
    lowered = value.strip().lower()
    if lowered in ("true", "false"):
        return "bool"
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        pass
    return "string"


def _column_dtype(seen: set) -> str:
    """Combine the set of per-cell classifications observed for one column
    (plus "empty" for blank cells) into a single inferred dtype. Pure
    function over a set of labels -- no field name or domain knowledge.
    """
    non_empty = seen - {"empty"}
    if not non_empty:
        return "empty"
    if non_empty == {"bool"}:
        return "boolean"
    if "string" in non_empty:
        return "string"
    if "float" in non_empty:
        return "float"
    if non_empty == {"int"}:
        return "integer"
    return "string"


def _scan_csv_file(path: Path) -> dict:
    """Single-pass header + dtype-inference + row-count scan of one CSV.

    Reads with utf-8-sig (matches read_csv() elsewhere in this codebase) and a
    plain comma delimiter -- every file compare_cross_segment.py writes via
    atomic_write_csv() is comma-delimited, so no delimiter sniffing is needed
    here (unlike a general-purpose inventory tool over an arbitrary pipeline
    output folder). Never stores a row or a cell value beyond the single pass
    used to update each column's running dtype-classification set --
    "type of data, not shape of values": no sample rows are retained or
    returned.
    """
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return {"columns": [], "row_count": 0, "empty_file": True, "parse_error": None}
            seen_by_col = [set() for _ in header]
            row_count = 0
            for row in reader:
                row_count += 1
                for i in range(len(header)):
                    cell = row[i] if i < len(row) else ""
                    seen_by_col[i].add("empty" if cell.strip() == "" else _classify_scalar(cell))
            columns = [
                {"name": name, "inferred_dtype": _column_dtype(seen_by_col[i])}
                for i, name in enumerate(header)
            ]
            return {"columns": columns, "row_count": row_count, "empty_file": False, "parse_error": None}
    except Exception as e:  # noqa: BLE001 -- reported per-file, scan continues for the rest
        return {"columns": [], "row_count": 0, "empty_file": False, "parse_error": f"{type(e).__name__}: {e}"}


def inventory_export_directory_files(scan_dirs: list, known_paths: set) -> list:
    """Live directory scan: every *.csv file actually present under scan_dirs
    that is NOT already one of known_paths (every path this generator already
    reads as an input, writes as an output, or tracks as a sibling artifact --
    see build_evidence_map()). Pure filesystem read -- no interpretation of
    file content beyond the structural facts _scan_csv_file() returns.

    This is deliberately live/computed, not a hand-maintained filename list:
    a future compare_cross_segment.py export nobody has wired an artifact_id
    for yet is picked up automatically the next time this runs, with no code
    change required here.
    """
    known_resolved = {p.resolve() for p in known_paths if p}
    seen_resolved = set()
    entries = []
    for scan_dir in scan_dirs:
        if not scan_dir or not scan_dir.is_dir():
            continue
        for path in sorted(scan_dir.glob("*.csv")):
            resolved = path.resolve()
            if resolved in known_resolved or resolved in seen_resolved:
                continue
            seen_resolved.add(resolved)
            scan = _scan_csv_file(path)
            entries.append({
                "filename": path.name,
                "path": str(path),
                **scan,
            })
    return entries


def build_file_inventory_document(
    *,
    schema_version: str,
    scanned_directories: list,
    files: list,
) -> dict:
    """Pure envelope wrapper (matches build_findings_document()'s convention):
    files is already fully built (scan + narrative attached) by the caller;
    this function performs no filesystem I/O and no further computation.
    """
    return {
        "schema_version": schema_version,
        "generated_at": _utc_now_iso(),
        "scanned_directories": [str(d) for d in scanned_directories],
        "file_count": len(files),
        "files": files,
    }
