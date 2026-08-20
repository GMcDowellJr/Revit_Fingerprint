# Repository AI and client-reference review

Date: 2026-08-20

## Scope and method

This review searched tracked text files for:

- AI-assistance and provenance terms (`ChatGPT`, `OpenAI`, `Anthropic`,
  `Claude`, `Copilot`, `Gemini`, `LLM`, and related phrases); and
- known organization/client terms (`Stantec`, `Sutter`, `Kaiser`, and
  `Permanente`).

The search intentionally excluded `CLAUDE.md`, the checked-in Graphify skill
copies under `.agents/skills/` and `.copilot/skills/`, and generated
`graphify-out/` content. Those files explicitly document assistant operation
and would obscure application and corpus findings. Generic uses of the words
`client` and `customer` were not counted because `client` is a first-class
governance concept throughout this project and is not evidence of a specific
organization.

Counts below are literal, case-insensitive substring counts in the current
tracked snapshot. They are an inventory aid, not a count of unique facts.

## Baseline findings

The findings and large counts in this section describe the pre-remediation
snapshot. They are retained to explain the remediation decision and are not a
description of the current operational tree. The final tracked-tree inventory
is recorded below under **Final acceptance inventory**.

### 1. AI-review provenance remains in otherwise ordinary test source

Four comments/docstrings in two test modules attribute regression cases to
`chatgpt-codex-connector` and a pull-request number:

- `tests/test_bundle_analysis_name_projection.py` (one occurrence); and
- `tests/test_run_segment_orchestrator_name_projection.py` (three
  occurrences).

These annotations are not needed to explain the behavior under test. If the
repository is intended to be neutral about authorship or tooling, replace them
with descriptions of the regression scenario while retaining the test logic.

### 2. Other LLM references describe product behavior, not coding assistance

The repository contains a substantial, intentional label-synthesis workflow:

- `tools/label_synthesis/synthesize_fragmented_labels.py` implements provider
  selection and model calls;
- `tools/label_synthesis/label_resolver.py` records `llm` and
  `llm_unreviewed` provenance states;
- `tools/label_synthesis/build_semantic_groups.py` assembles prompts and has
  an explicitly unimplemented model-call boundary; and
- governance documentation repeatedly distinguishes deterministic artifacts
  from possible LLM-authored interpretation.

These references are functional or explanatory and should not be removed as
AI-assistance commentary. The Graphify workflow and `.claude`/`.copilot`
configuration are likewise tooling integration rather than hidden authorship
commentary. `.github/workflows/graphify.yml` also installs the OpenAI package
and configures an OpenAI-compatible model identifier; that is operational
configuration and should remain if the workflow remains supported.

### 3. Specific organization names are pervasive

After the exclusions above, the tracked snapshot contains:

| Term | Literal occurrences | Files |
| --- | ---: | ---: |
| `Stantec` | 637 | 39 |
| `Sutter` | 266 | 20 |
| `Kaiser` | 472 | 19 |
| `Permanente` | 49 | 6 |

The references are not confined to examples:

- **Production behavior:** `runner/thin_runner.py` and
  `runner/probe_thin_runner.py` contain Stantec-specific local paths.
  `tools/governance_manifest.py`, `tools/build_segment_manifest.py`, and
  `tools/compare_cross_segment.py` use Stantec as the enterprise identity and
  use Sutter/Kaiser in behavioral examples. `domains/identity.py` documents
  and extracts a Stantec-specific shared parameter.
- **Policy data:** `policies/client_sector.csv` maps Kaiser and Sutter to the
  healthcare sector.
- **Tests:** most literal occurrences are fixtures and assertions across the
  governance, manifest, lineage, classification, and narrative suites. The
  highest concentrations include
  `tests/test_build_segment_manifest.py`,
  `tests/test_generate_governance_narrative_classification.py`, and
  `tests/test_compare_cross_segment_cross_client.py`.
- **Commands and documentation:** checked-in command transcripts and planning
  documents contain organization-specific paths and sample labels, including
  `tools/archetype/archetype_commands.txt`, `tools/Python Commands.txt`, and
  `tools/Powershell Commands.txt`.
- **Captured probe data:** the three tracked JSON files under
  `tools/probes/Exports/` and their inventory/crosswalk files contain client
  names, project names, company names, user-local paths, and organization
  standard names. This is the highest-risk category because these are captured
  records rather than synthetic examples. For example, the exports include a
  named Kaiser Permanente hospital project and a Windows user path under a
  Stantec OneDrive directory.
- **Embedded Dynamo artifacts:** `Revit fingerprint MVP.dyn` and
  `tools/probes/fingerprint_probe.dyn` also contain Stantec-specific values.

### 4. The issue is broader than a text-only client-name substitution

Blindly replacing the named organizations would make tests pass only after a
large fixture rewrite, but it would not adequately sanitize the captured
exports. Project titles, personal filesystem paths, business-center labels,
custom parameters, and organization-standard object names can remain
identifying without containing one of the four search terms. The export corpus
therefore needs field-aware review or removal, not just search-and-replace.

## Files that can be pulled from the operational repository

The following disposition is based on static path-reference checks across the
tracked tree and a separate check of `tests/` and `.github/workflows/`. A file
being mentioned in a comment or historical note does not make it an execution
dependency. Conversely, generated content that is deliberately published by a
workflow is called out rather than mislabeled as unused.

### Pull now: captured and derived probe exports

Remove all nine tracked files under `tools/probes/Exports/` from the normal Git
repository:

- `tools/probes/Exports/probes_2025_20260805T110757-84cfc9.json`
- `tools/probes/Exports/probes_2025_20260805T111532-d738fd.json`
- `tools/probes/Exports/probes_2025_20260805T120010-8cf8cf.json`
- `tools/probes/Exports/PROBE_INVENTORY.csv`
- `tools/probes/Exports/PROBE_INVENTORY.md`
- `tools/probes/Exports/PROBE_CROSSWALK.csv`
- `tools/probes/Exports/PROBE_CROSSWALK.md`
- `tools/probes/Exports/CROSSWALK_CANDIDATES.csv`
- `tools/probes/Exports/CROSSWALK_CANDIDATES.md`

Together these files occupy approximately 79.25 MiB in the checkout. No test
opens these committed files: probe-inventory tests generate temporary inputs
and outputs instead. The inventory, crosswalk, and candidate files are derived
by `tools/probes/build_probe_inventory.py` and
`tools/probes/find_crosswalk_candidates.py`; they can be regenerated from an
approved external probe dataset. Some source comments and contract history cite
the committed runs as evidence, but none loads them at runtime. Retain an
access-controlled source dataset or sanitized fixture outside this repository
if reproducibility is required.

Removing these files should be paired with an ignore rule for
`tools/probes/Exports/` (optionally preserving a small README that explains how
to obtain approved inputs). Because sensitive values already exist in Git
history, a normal deletion commit is insufficient if the repository is being
prepared for wider distribution.

### Pull now: generated report and workstation command transcripts

These four standalone artifacts are not read by application code or tests:

- `tools/compare_templates_stand-alone/compare_view_templates_stand-alone_report.html`
  (approximately 2 MiB of generated report output)
- `tools/Python Commands.txt`
- `tools/Powershell Commands.txt`
- `tools/archetype/archetype_commands.txt`

The command files are informal execution transcripts with organization- and
workstation-specific paths. Any still-useful procedure should be rewritten as
a parameterized runbook before deletion; the existing files should not be kept
as operational documentation. Removing them requires updating the descriptive
references in `CLAUDE.md` and `REPO_OPERATIONAL_REVIEW.md` so those documents do
not advertise a deleted command file.

### Retain as classified historical material: historical audits

The following 15 files under `audit_results/` are review snapshots, not runtime
inputs or test fixtures:

- `audit_1_archetype_integration.md`
- `audit_2_label_synthesis.md`
- `audit_3_extraction_contracts.md`
- `audit_4_bundle_crossseg_structural.md`
- `audit_5_identity_hashing_inventory.md`
- `audit_6_name_key_step0_within_pr1.md`
- `audit_7_name_key_agreement_and_cli_naming.md`
- `audit_8_bundle_pipeline_name_projection.md`
- `audit_9_segment_orchestrator_name_projection.md`
- `audit_10_bundle_bi_output_location_correction.md`
- `audit_11_domain_extractor_delta_step0_findings.md`
- `audit_12_pattern_generation_fork_unification_step0.md`
- `audit_13_identity_items_monolithic_vs_shard_step0.md`
- `audit_14_identity_items_shard_port_pr2.md`
- `audit_15_identity_items_monolithic_removal_pr4.md`

They total approximately 0.30 MiB. The completed decision is to retain this set in
place as explicitly classified historical material. `audit_results/README.md` marks
its 2024–2025/2.1-era scope and non-operational status. Durable name-projection,
bundle-adapter, and extractor rationale is consolidated in `DECISIONS.md` D-037 and
maintained implementation comments; production code, tests, policies, and operator
runbooks no longer cite an audit to explain current correctness. Historical changelog
and audit-to-audit citations remain because their targets are retained. The tracked
checker `scripts/check_audit_references.py` distinguishes a bare directory/section
reference from a file path and rejects every nonexistent referenced file.

### Do not classify as removable without a workflow decision

- `graphify-out/` is generated and already ignored by `.gitignore`, but
  `.github/workflows/graphify.yml` deliberately force-adds and publishes five
  tracked outputs. These are unnecessary for the Revit fingerprint runtime and
  pytest suite, but are required by the repository's currently documented
  knowledge-graph workflow. Pull them only together with a decision to publish
  Graphify artifacts solely as workflow artifacts (and corresponding updates
  to `AGENTS.md`, `CLAUDE.md`, and Copilot instructions).
- `docs/research/*.csv` and
  `docs/method_invocation_candidates_annotated.csv` are design/research inputs,
  not runtime data. They are referenced extensively by probe implementation
  comments and maintained research documents. They may be archived after their
  accepted conclusions are represented in contracts, but the static review is
  not sufficient to assert that they have no remaining engineering value.
- `domains/graph_2024.json`, `domains/graph_2025.json`, and
  `domains/graph_2026.json` are versioned domain configuration, not captured
  output, despite their size and JSON format. They should remain.
- `Revit fingerprint MVP.dyn` and `tools/probes/fingerprint_probe.dyn` are
  executable Dynamo entry points. Their organization-specific defaults should
  be parameterized or sanitized, but deleting them would affect operation.

### Completed removal boundary

The completed current-tree removal set was the nine files in
`tools/probes/Exports/`, the standalone comparison HTML, and the three command
transcripts: 13 files and approximately 81.3 MiB. The 15 audits are a second
set that was reviewed and deliberately retained after durable-reference
cleanup, as documented above and in `audit_results/README.md`. This boundary
avoids deleting versioned contracts, executable Dynamo graphs, test fixtures,
or artifacts that the current Graphify workflow explicitly publishes.

## Immutable enterprise policy boundary

Enterprise identity classification uses the frozen `EnterprisePolicy` value in
`tools/enterprise_policy.py`. Loader precedence is CLI label override, then a
deployment-local JSON policy, then the checked-in synthetic `InternalEnterprise`
default. The bookkeeping token remains the separate fixed `0000` value: only the
configured label plus `0000` is enterprise; that label plus a real BC is
business-center scope, and an external client plus `0000` is never enterprise.

Production classification APIs in `governance_manifest.py` and
`compare_cross_segment.py` require explicit policy propagation, including pair
discovery, multiprocessing workers, governance-state construction, serializers,
and pooled rows. Provenance contains schema, effective label, token, source, and
a deterministic safe `configuration_identifier`; the absolute local policy path
is memory-only. Relationship and narrative tools preserve already-classified
scope columns, promotion analysis consumes comparison classifications, and
segment construction/extraction orchestration preserve literal identity metadata;
these consumers do not independently classify enterprise identity.

## Completed remediation sequence

1. **Captured probe exports:** removed the tracked raw exports and derived
   inventories/crosswalks and added an appropriate ignore rule. No history
   rewrite was executed; any future rewrite remains owner-gated by
   `docs/repository-history-remediation-runbook.md`.
2. **Configuration and organization identity:** moved enterprise
   labels, organization folders, shared-parameter names, and sector mappings
   into local or deployment configuration with documented generic defaults.
3. **Synthetic tests:** replaced organization fixtures with names
   such as `InternalEnterprise`, `ClientAlpha`, and `ClientBeta`, while keeping
   explicit enterprise/client/business-center boundary cases.
4. **Provenance-only commentary:** removed the four
   `chatgpt-codex-connector` attributions while preserving the useful
   regression descriptions.
5. **Broad rescan:** searched for email addresses, user
   profile paths, project numbers/titles, URLs, company suffixes, and known
   business-center names in addition to the organization-name list used here.

## Final acceptance inventory

The final case-insensitive substring inventory, reproduced from tracked files
with `git ls-files` and excluding only `CLAUDE.md`, `.agents/skills/**`,
`.copilot/skills/**`, and `graphify-out/**`, is:

| Term | Baseline occurrences / files | Final occurrences / files |
| --- | ---: | ---: |
| `Stantec` | 637 / 39 | 38 / 7 |
| `Sutter` | 266 / 20 | 7 / 3 |
| `Kaiser` | 472 / 19 | 10 / 4 |
| `Permanente` | 49 / 6 | 5 / 3 |

Every final occurrence is in a classified retained location: the non-operational
`CHANGELOG.md` or `DECISIONS.md`; archived `audit_results/**`; this historical
inventory; a negative regression assertion; or archived analysis code. None is
an executable default or maintained organization-specific production policy.

## Baseline review conclusion

The repository has very little non-obvious AI-assistance commentary: the only
clear provenance annotations found outside assistant/tooling documentation are
four test comments. In contrast, organization-specific information is deeply
embedded in runtime assumptions, policy, tests, documentation, and captured
data. The raw probe exports deserve immediate handling before a mechanical
rename of source fixtures, because they contain richer client and user context
than the named-term counts reveal.
