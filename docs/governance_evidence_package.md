# Governance Evidence Package

`tools/generate_governance_narrative.py` produces a deterministic governance
report from `compare_cross_segment.py`'s comparison outputs. Historically that
report conflated several different epistemic roles in one narrative document:
deterministic evidence, package-health/coverage reporting, an interpretation
guide, a findings store, and executive narrative prose, with no explicit
statement of which part of the output carries which kind of authority.

This document describes the **evidence-package layer** added around that
generator: a package manifest, package health report, evidence map,
structured findings, and externalized governance policy, emitted as
machine-readable JSON alongside the existing CSV/Markdown outputs. See
`DECISIONS.md` D-019 (package manifest/health/evidence-map), D-020
(structured findings), and D-021 (policy externalization) for the decision
records, and `CHANGELOG.md` `[Unreleased]` for the change entries.

**This is an incremental refactor, delivered in phases.** Phase 1 (D-019)
added the package manifest/health/evidence-map. Phase 2 (D-020) added
structured findings (`governance_findings.json`). Phase 3 (D-021) added
policy externalization (`policies/governance/*.json`), described below.
None of these phases changed any existing classification, scoring, or CSV
column — every threshold/domain-list/guidance-text value the policy layer
now reads from JSON reproduces this generator's pre-externalization Python
literal exactly, so no existing invocation's output changes by default.
Still deferred to a future phase: a stable interpretation guide, candidate
question routes, and a narrower run-specific brief (see task section 17,
"PR4").

## Design reference, not a dependency

The artifact shapes and authority-level vocabulary below are modeled on the
discovery-scaffold patterns in the `GMcDowellJr/llm_evidence_framework`
repository (`patterns/deterministic_to_llm_boundary.md`,
`discovery/evidence_map_discovery.md`). That repository explicitly states it
is **not yet a finalized standard, schema, or implementation contract** — it
is a field notebook. Revit Fingerprint does not import from, or have any
runtime dependency on, that repository. `tools/governance_evidence_package.py`
defines its own independent copies of the vocabulary that happen to match its
naming, for cross-tool legibility only.

## Authority levels

Every artifact in the package is tagged with one of five authority levels:

| Level | Meaning |
|---|---|
| `authoritative_deterministic_evidence` | A directly computed or directly observed fact. Wins any disagreement with a lower-authority artifact. |
| `controlled_interpretation` | A deterministic computation that applies editorial framing or classification rules on top of authoritative evidence (e.g. the narrative's tier prose). |
| `convenience_summary` | A lower-stakes descriptive summary not itself load-bearing for conclusions (e.g. `matrix_output_manifest.csv`, which is metadata-only today). |
| `user_provided_note` | A human-curated business fact that cannot be derived from the pipeline's own data (e.g. `client_sector.csv`). |
| `llm_generated_provisional_interpretation` | Not produced anywhere in this package — no LLM is involved in generating any artifact described here. Reserved for a future conversational layer. |

**Authority ordering** (highest to lowest): package health and the source
comparison CSVs (`cross_segment_summary.csv`, `cross_segment_pooled.csv`) >
the deterministic rollup CSVs (`governance_domain_summary.csv`,
`governance_client_summary.csv`) > `governance_narrative_context.md`'s prose.
If the narrative disagrees with a rollup CSV or a source CSV, the CSV wins.
This ordering is stated explicitly in the narrative's own new authority
header section.

## Package schema constants

Defined in `tools/governance_evidence_package.py`:

```python
PACKAGE_TYPE = "governance_evidence_package"
PACKAGE_SCHEMA_VERSION = "1.0"
EVIDENCE_MAP_SCHEMA_VERSION = "1.0"
FINDINGS_SCHEMA_VERSION = "1.0"
GENERATOR_IDENTITY = "generate_governance_narrative.py"
GENERATOR_ROLE = "deterministic_governance_narrative_generator"
```

`PACKAGE_SCHEMA_VERSION` can be overridden per-run via `--package-schema-version`
(e.g. for a future schema revision) without touching the module's own default.

## Generated JSON artifacts

### `governance_package_manifest.json`

Authority: `authoritative_deterministic_evidence`. A provenance record: which
inputs were provided on the CLI and found on disk, which outputs were
written and their sizes, and the `comparison_run_id`/`executed_utc` values
actually observed in the loaded `cross_segment_summary.csv` rows. It never
claims a content hash or source-run identifier that isn't actually present
in the loaded rows — unknown fields are recorded as empty lists, not
invented.

### `governance_package_health.json`

Authority: `controlled_interpretation` (it's a computed coverage judgment,
not a raw fact). Minimum fields:

- `overall_status` — `"complete"` (no missing required inputs, no warnings),
  `"degraded"` (required inputs present, but a warning condition — used-view
  fallback, unrecognized comparison_type, missing client-sector file — was
  observed), or `"invalid"` (a required input is missing).
- `required_inputs` / `optional_inputs` — booleans per artifact_id.
- `schema_detection` — `"dual"` / `"single"` / `"none"`, from
  `detect_bundle_schema()`.
- `used_view_fallback` / `fallbacks_used` — from
  `used_view_falls_back_to_legacy()`.
- `comparison_type_coverage` — per aggregation function
  (`build_cascade`, `build_governance_state_summary`), each with
  `seen` / `recognized` / `intentionally_excluded` / `unrecognized`
  comparison_type values.
- `client_sector_status` — one of `explicit_path`, `default_path_resolved`,
  `default_path_missing`, `explicit_path_missing`.
- `scope_coverage` — currently a factual inventory of `unit_system` values
  observed in `cross_segment_summary.csv`. Deterministic
  comparable/weakly_comparable/not_comparable gating (task spec §10) is
  deferred to a future PR.
- `matrix_manifest` — presence, row count, and the raw `matrix_name` values
  seen. `matrix_output_manifest.csv` has no structured block/status column
  today, so this generator does not classify per-matrix blocking status.
- `blocking_conditions` / `warnings` — structured, flag-driven entries
  (`condition` + `detail`), never free-form severity prose.

**Wording constraint:** every `detail` string in `blocking_conditions` and
`warnings`, and every `known_limitations` entry in the evidence map, is a
mechanical, factual statement about what the code does — citing a specific
function, line, or `docs/governance_narrative_scope_gap_audit.md` finding ID
where relevant. None of this text makes an impact or severity judgment (e.g.
never "this may produce misleading results"). Severity/impact judgment is
left to a human reader, or to `governance_findings.json`'s structured
findings (each carries `limits[]`, but a `finding_type`/`rule_ids[]` is
still not a severity score).

### `governance_findings.json`

Authority: `controlled_interpretation`. Structured, rule-derived findings
(see D-020 and "Structured findings" below) with epistemic provenance --
`origin`/`fidelity`/`authority_level`/`limits` per finding -- and
`support[]` references back to specific `governance_domain_summary.csv`/
`governance_client_summary.csv` rows and fields. Built by
`build_structured_findings()` in `generate_governance_narrative.py`
(domain-governance classification logic; this stays outside
`governance_evidence_package.py` per that module's own separation-of-concerns
convention) and wrapped in a schema-versioned envelope by
`build_findings_document()`.

### `governance_evidence_map.json`

Authority: `authoritative_deterministic_evidence` (a structural fact about
the package, not an interpretation). One entry per artifact, 19 total:

**Source artifacts consumed via CLI** (2 required, 8 optional):
`cross_segment_summary.csv`, `cross_segment_pooled.csv`,
`cross_segment_governance_states.csv`, `cross_segment_governance_state_summary.csv`,
`cross_segment_delta.csv`, `file_metadata.csv`, `client_sector.csv`,
`cross_segment_union_inventory.csv`, `pattern_reuse_distribution.csv`,
`matrix_output_manifest.csv`.

**Sibling artifacts, never consumed by this generator** (2): `cross_segment_file_pairs.csv`
and `comparison_registry.csv` — both written by `compare_cross_segment.py`'s
`main()` to the same run directory as `cross_segment_summary.csv`, but this
generator has no CLI argument for either and never opens or parses them.
Their path is inferred as a sibling of `--summary`'s directory; `present` is
computed via `Path.exists()` only. See
`docs/governance_generator_cross_compare_coverage.md` for the recommended
future integration points (drill-through appendix for file pairs;
completeness/staleness reporting for the comparison registry).

**Generated artifacts** (7): the three existing outputs
(`governance_domain_summary.csv`, `governance_client_summary.csv`,
`governance_narrative_context.md`) plus the four new JSON artifacts
described above (`governance_package_manifest.json`,
`governance_package_health.json`, `governance_findings.json`,
`governance_evidence_map.json`), including a self-entry for
`governance_evidence_map.json` itself (`related_artifacts` lists all 18
other artifact IDs).

The four `policies/governance/*.json` policy-profile files described below
are **not** separate evidence-map entries in this phase — they are policy
*inputs* to classification (recorded by `profile_id`/`schema_version`/
`source` in `governance_package_manifest.json`'s `policy_profiles.profiles`
and in `governance_package_health.json`'s `policy_load_status`), not
per-run corpus evidence artifacts.

Each entry carries `artifact_id`, `path`, `artifact_type`, `required`,
`producer`, `authority_level`, `context_role`, `grain`, `key_fields`,
`identifiers`, `join_keys`, `can_answer`, `cannot_answer`,
`known_limitations`, `null_semantics`, and `related_artifacts` — matching
the candidate evidence-map field list in
`llm_evidence_framework/discovery/evidence_map_discovery.md`.

## Documented-but-not-fixed limitations

These are recorded in the evidence map's `known_limitations` fields rather
than fixed by the phases delivered so far — fixing them would change
classification output, which none of D-019/D-020/D-021 do:

- **A2 (pool_scope filtering)** — `build_client_summary()` reads
  `pooled_rows` across every `pool_scope` value without filtering by it.
  This is currently safe only because the specific fields it reads
  (`client_label`, `n_files_focal`) happen to be pool-scope-invariant, not
  because `pool_scope` is checked at the read site. See
  `docs/governance_narrative_scope_gap_audit.md` finding A2.
- **Missing-value cell convention inconsistency** — `governance_domain_summary.csv`
  renders a present-but-`None` numeric field as the em-dash `"—"` string
  (via `fmt()`/`pct()`), but a governance-state-sourced column for a domain
  with no `governance_state_summary` entry at all renders as `""` (empty
  string). Two different "missing" conditions use two different cell
  values in the same CSV.
- **C8 (no canonical domain-label contract)** — `DOMAIN_LABELS` in
  `generate_governance_narrative.py` is the sole source of human-readable
  domain display names; no contract file (e.g.
  `contracts/domain_identity_keys_v2.json`) carries this today.
- **`matrix_output_manifest.csv` has no structured status field** —
  `MATRIX_MANIFEST_FIELDS` has `known_limitations`/`interpretation` free-text
  columns but no `status`/`blocked` enum, so package health cannot report
  per-matrix blocking status mechanically today.

## Structured findings

`governance_findings.json` (D-020) turns the tier/anomaly/onboarding
classifications that used to exist only as narrative prose into structured
records. `build_structured_findings()` builds one finding per
(subject, rule) match for ten required categories (`baseline_candidate`,
`strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
`active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
`passive_inheritance_risk`, `missing_or_degraded_evidence`,
`leadership_question`). `_classify_domains_for_findings()` is the single
source of truth for the tier-derived buckets, shared by
`build_structured_findings()` and `render_findings_and_recommendations()` —
the JSON findings and the narrative's "Key Findings" prose can no longer
independently drift.

Each finding carries `finding_id`, `subject` (`type`/`id`), `finding_type`,
`status`, `origin`, `fidelity`, `authority_level`, `summary`, `support[]`
(`artifact_id`/`selector`/`fields`, resolving back to a specific
`governance_domain_summary.csv`/`governance_client_summary.csv` row),
`rule_ids[]` (documented in `finding_rules.json`, see below), and `limits[]`.
Leadership questions are marked `status: question_not_claim` /
`authority_level: convenience_summary` — a suggested review question, never
an observed result.

## Policy profiles and threshold profiles

Implemented (D-021). Governance thresholds, domain-governance policy, and
client-onboarding interpretation rules that used to be Python literals in
`generate_governance_narrative.py` now live in four JSON profiles under
`policies/governance/`, loaded via a new sibling module,
`tools/governance_policy.py`:

| Profile file | Contents |
|---|---|
| `governance_thresholds.json` | Reliability-band cutoffs (`score_reliability()`), tier-assignment bands (`assign_tier()`), cross-client convergence/low thresholds, client alignment/coherence/confidence-band thresholds. |
| `domain_governance_policy.json` | `excluded_from_scoring` (domains excluded from aggregate scoring), `passive_inheritance_risk_domains`, per-domain `domain_guidance` text (`phases`, `loaded_family_types`), and `static_findings_guidance` (always-rendered findings-section prose). |
| `client_onboarding_policy.json` | `_client_onboarding_profile()`'s interpretation thresholds. Kept as its own profile, separate from `governance_thresholds.json`, even where a value numerically coincides with a governance-tier threshold today — these gate onboarding narrative text, not `governance_tier`, and the two are allowed to diverge independently in a future change. |
| `finding_rules.json` | Documentation-only `rule_id → {finding_type, description}` metadata for D-020's `governance_findings.json` `rule_ids[]`. Never drives classification logic — the rule_id constants and the classification rules themselves stay in `generate_governance_narrative.py`. |

Each file follows the `profile_id` + `schema_version` + `notes` +
content shape already used elsewhere in `policies/` (e.g.
`policies/governance_role_path_patterns.json`).

**How it's wired in:** `tools/governance_policy.py`'s `load_governance_policy()`
is mechanical load/fallback only — it owns no governance business content.
`generate_governance_narrative.py` defines `_POLICY_DEFAULTS` (a Python
mirror of the shipped JSON, built from the same `_DEFAULT_*` constants the
module's threshold names were originally initialized from) as the per-file
fallback when a profile file is absent from `--policy-dir`. `main()` calls
`load_governance_policy()` then `apply_governance_policy()` before any
classification runs; `apply_governance_policy()` reassigns every module-level
constant this file's existing functions already read as plain globals
(`EXCLUDED_FROM_SCORING`, `PASSIVE_INHERITANCE_RISK_DOMAINS`,
`DOMAIN_GUIDANCE`, `STATIC_FINDINGS_GUIDANCE`, and ~25 threshold constants).
No existing function body or call site changed — only the *source* of each
constant's value did, from a Python literal to a policy-file-or-fallback
lookup.

**`--policy-dir` default and behavior:** defaults to `policies/governance/`
(resolved relative to `tools/`, same convention as `--client-sector`'s
default), so every existing invocation that doesn't pass `--policy-dir`
still reads the shipped profiles — which reproduce this generator's
pre-externalization Python literals value-for-value, so classification
output is unchanged by default. A profile file missing from the given
`--policy-dir` falls back, per file, to this generator's own built-in
default for that profile only (not an error); pointing `--policy-dir` at a
nonexistent directory falls back for all four. `governance_package_health.json`'s
`policy_load_status` field and a `governance_policy_profile_defaulted`
warning (which degrades `overall_status` to `degraded`) report exactly which
profiles fell back; `governance_package_manifest.json`'s
`policy_profiles.profiles` records the resolved `profile_id`/
`schema_version`/`source` (`policy_file` vs. `built_in_default`) for all
four, whether or not any fell back.

**What's still deferred:** `DOMAIN_LABELS` (human-readable domain display
names) is not part of this policy layer — it's a display-name contract gap
(the evidence map's existing C8 known-limitation), not a governance
threshold or rule, and remains a Python literal. The detailed
comparability-gating thresholds in task section 10 (imperial/metric,
all-view/used-view, pool-scope mixing) and the narrower detect_anomalies()
divergence-note thresholds (the various `>= 0.25` absolute-gap checks, the
passive-indicator/bundle-density band checks) are also not externalized in
this phase — the phase's scope was the thresholds that directly gate
`governance_tier`/structured findings, per the task's own incremental
"do not attempt all of the following at once" guidance.

## CLI reference

```text
--summary CROSS_SEGMENT_SUMMARY_CSV      (required, unchanged)
--pooled CROSS_SEGMENT_POOLED_CSV        (required, unchanged)
... (all existing optional flags, unchanged) ...
--policy-dir DIR                         optional; default policies/governance/
                                          (see "Policy profiles" above)
--package-schema-version VERSION         optional; default 1.0
--emit-evidence-package                  default: on
--no-emit-evidence-package               suppresses the 4 new JSON outputs only;
                                          existing CSV/MD outputs are unaffected
```

No existing invocation needs to change — `--emit-evidence-package` defaults
to on, so every existing caller starts producing the four new JSON files
(manifest, health, evidence map, findings) with no CLI change required, and
`--policy-dir` defaults to the shipped `policies/governance/` profiles,
which reproduce pre-externalization output exactly. Pass
`--no-emit-evidence-package` to opt out of the JSON outputs, or
`--policy-dir` to point at a different (or partial, or nonexistent) policy
profile set.

## Recommended LLM navigation

A human or LLM analyzing a governance package produced by this generator
should normally load, in order:

1. `governance_package_health.json` — is this package usable at face value?
   (includes `policy_load_status` — did every policy profile actually load,
   or is a profile running on this generator's built-in default?)
2. `governance_evidence_map.json` — which artifact answers the question at hand?
3. `governance_package_manifest.json` — what inputs actually fed this run,
   and which `policy_profiles.profiles` (`profile_id`/`schema_version`/
   `source`) were applied?
4. `governance_findings.json` — which domains/clients meet a specific named
   rule, and what CSV rows/fields support that classification?
5. Relevant rows from `governance_domain_summary.csv` / `governance_client_summary.csv`.
6. `governance_narrative_context.md` only for framing/prose context — never
   as the sole source for a claim that a CSV or `governance_findings.json`
   can verify.

The full evidence archive (source comparison CSVs, sibling
`cross_segment_file_pairs.csv`/`comparison_registry.csv`) should be pulled
only when a question requires drill-down or verification beyond what the
rollup CSVs and evidence map already answer.
