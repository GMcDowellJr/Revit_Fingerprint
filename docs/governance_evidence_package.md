# Governance Evidence Package

`tools/generate_governance_narrative.py` produces a deterministic governance
report from `compare_cross_segment.py`'s comparison outputs. Historically that
report conflated several different epistemic roles in one narrative document:
deterministic evidence, package-health/coverage reporting, an interpretation
guide, a findings store, and executive narrative prose, with no explicit
statement of which part of the output carries which kind of authority.

This document describes the **evidence-package layer** added around that
generator: a package manifest, package health report, and evidence map,
emitted as machine-readable JSON alongside the existing CSV/Markdown outputs.
See `DECISIONS.md` D-019 for the decision record and `CHANGELOG.md`
`[Unreleased]` for the change entry.

**This is Phase 1 ("PR1") of a broader, incremental refactor.** Structured
findings (`governance_findings.json`) and policy externalization (moving
thresholds and domain-governance rules out of Python and into
`policies/governance/`) are explicitly deferred to later phases. Nothing in
this phase changes any existing classification, scoring, or CSV column.

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
left to a human reader, or to a future `governance_findings.json`.

### `governance_evidence_map.json`

Authority: `authoritative_deterministic_evidence` (a structural fact about
the package, not an interpretation). One entry per artifact, 18 total:

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

**Generated artifacts** (6): the three existing outputs
(`governance_domain_summary.csv`, `governance_client_summary.csv`,
`governance_narrative_context.md`) plus the three new JSON artifacts
described above, including a self-entry for `governance_evidence_map.json`
itself (`related_artifacts` lists all 17 other artifact IDs).

Each entry carries `artifact_id`, `path`, `artifact_type`, `required`,
`producer`, `authority_level`, `context_role`, `grain`, `key_fields`,
`identifiers`, `join_keys`, `can_answer`, `cannot_answer`,
`known_limitations`, `null_semantics`, and `related_artifacts` — matching
the candidate evidence-map field list in
`llm_evidence_framework/discovery/evidence_map_discovery.md`.

## Documented-but-not-fixed limitations (Phase 1)

These are recorded in the evidence map's `known_limitations` fields rather
than fixed in this phase — fixing them would either change classification
output (out of scope for PR1) or require policy externalization (PR3):

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

## Policy profiles and threshold profiles

Not yet implemented. `--policy-dir` is accepted on the CLI and recorded
verbatim in `governance_package_manifest.json`'s `policy_profiles.policy_dir`
field for forward-compatibility auditing, but nothing in this generator
reads it yet. A future PR will externalize `DOMAIN_LABELS`,
`PASSIVE_INHERITANCE_RISK_DOMAINS`, `EXCLUDED_FROM_SCORING`, and the tier/
convergence/coherence thresholds currently hardcoded in
`generate_governance_narrative.py` into `policies/governance/*.json`,
following the `schema_version` + `notes` + ordered-rules shape already used
by `policies/governance_role_path_patterns.json`.

## CLI reference

```text
--summary CROSS_SEGMENT_SUMMARY_CSV      (required, unchanged)
--pooled CROSS_SEGMENT_POOLED_CSV        (required, unchanged)
... (all existing optional flags, unchanged) ...
--policy-dir DIR                         optional; recorded, not yet read
--package-schema-version VERSION         optional; default 1.0
--emit-evidence-package                  default: on
--no-emit-evidence-package               suppresses the 3 new JSON outputs only;
                                          existing CSV/MD outputs are unaffected
```

No existing invocation needs to change — `--emit-evidence-package` defaults
to on, so every existing caller starts producing the three new JSON files
with no CLI change required. Pass `--no-emit-evidence-package` to opt out.

## Recommended LLM navigation

A human or LLM analyzing a governance package produced by this generator
should normally load, in order:

1. `governance_package_health.json` — is this package usable at face value?
2. `governance_evidence_map.json` — which artifact answers the question at hand?
3. `governance_package_manifest.json` — what inputs actually fed this run?
4. Relevant rows from `governance_domain_summary.csv` / `governance_client_summary.csv`.
5. `governance_narrative_context.md` only for framing/prose context — never
   as the sole source for a claim that a CSV can verify.

The full evidence archive (source comparison CSVs, sibling
`cross_segment_file_pairs.csv`/`comparison_registry.csv`) should be pulled
only when a question requires drill-down or verification beyond what the
rollup CSVs and evidence map already answer.
