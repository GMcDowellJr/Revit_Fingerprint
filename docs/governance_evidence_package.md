# Governance Evidence Package

`tools/generate_governance_narrative.py` produces a deterministic governance
report from `compare_cross_segment.py`'s comparison outputs. Historically that
report conflated several different epistemic roles in one narrative document:
deterministic evidence, package-health/coverage reporting, an interpretation
guide, a findings store, and executive narrative prose, with no explicit
statement of which part of the output carries which kind of authority.

This document describes the **evidence-package layer** added around that
generator: a package manifest, package health report, evidence map,
structured findings, externalized governance policy, an interpretation/
routing layer, and a live file-availability inventory, emitted as
machine-readable JSON/Markdown alongside the existing CSV/Markdown outputs.
See `DECISIONS.md` D-019 (package manifest/health/evidence-map), D-020
(structured findings), D-021 (policy externalization), D-022
(interpretation guide, question routes, governance brief), and D-023 (live
file-availability inventory) for the decision records, and `CHANGELOG.md`
`[Unreleased]` for the change entries.

**This is an incremental refactor, delivered in phases.** Phase 1 (D-019)
added the package manifest/health/evidence-map. Phase 2 (D-020) added
structured findings (`governance_findings.json`). Phase 3 (D-021) added
policy externalization (`policies/governance/*.json`). Phase 4 (D-022)
added the interpretation/routing layer: a stable interpretation guide
(`docs/governance/governance_interpretation_guide.md`), a candidate question-route
catalog (`docs/governance/governance_question_routes.md`), and a narrower run-specific
brief (`governance_brief.md`), described below. Phase 5 (D-023) added a
live, computed-per-build directory of drill-down files the package doesn't
otherwise describe (`governance_file_inventory.json` + a new section in
`governance_brief.md`). Phase 6 (D-029) externalized `detect_anomalies()`/
the phases check/`_passive_inheritance_risk_domains()`/`_shape_note()`'s
remaining threshold literals to a fifth policy profile
(`anomaly_thresholds.json`), added `docs/governance/governance_classification_rules.md`
(the branch-order/exception-logic counterpart to the threshold *values* in
the JSON profiles), and trimmed `render_header()`'s restated metric
definitions to a pointer at the interpretation guide. None of these phases
changed any existing classification, scoring, or CSV column — every
threshold/domain-list/guidance-text value the policy layer reads from JSON
reproduces this generator's pre-externalization Python literal exactly, the
brief is a pure distillation of already-computed findings/health/
file-inventory data, and the file inventory only ever describes files this
generator does not otherwise read, so no existing invocation's
classification output changes by default. `governance_narrative_context.md`
is retained as a compatibility artifact — unchanged in content and role,
just no longer the only carrier of findings/navigation/interpretation.

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
the package, not an interpretation). One entry per artifact, 27 total:

**Source artifacts consumed via CLI** (2 required, 13 optional):
`cross_segment_summary.csv`, `cross_segment_pooled.csv`,
`cross_segment_governance_states.csv`, `cross_segment_governance_state_summary.csv`,
`cross_segment_delta.csv`, `file_metadata.csv`, `client_sector.csv`,
`cross_segment_union_inventory.csv`, `pattern_reuse_distribution.csv`,
`matrix_output_manifest.csv`, `pattern_reuse_summary_by_client.csv`,
`project_union_jaccard_matrix.csv`, `project_density_similarity_matrix.csv`,
`project_pool_containment_similarity_matrix.csv`,
`project_fragmentation_diagnostic.csv` (the last five feed the
`generate_governance_narrative.py` Project Portfolio section and adoption-
breadth cut added in PR B2 — see
`docs/governance_generator_cross_compare_coverage.md`).

**Sibling artifacts, never consumed by this generator** (6, D-024 grew this
from 4): `cross_segment_file_pairs.csv`, `comparison_registry.csv`,
`pattern_reuse_summary_by_domain.csv`, and `project_mean_file_pair_jaccard_matrix.csv`
— all four written by `compare_cross_segment.py`'s `main()` to the same run
directory as `cross_segment_summary.csv`, but this generator has no CLI
argument for any of them and never opens or parses their rows. Their path is
inferred as a sibling of `--summary`'s directory; `present` is computed via
`Path.exists()` only. `columns`/`row_count`, when present, come from a live
scan (`_sibling_scan_fields()`, reusing D-023's `_scan_csv_file()`) — a
structural fact about the header, not this generator reading a row. See
`docs/governance_generator_cross_compare_coverage.md` for the recommended
future integration points (drill-through appendix for file pairs;
completeness/staleness reporting for the comparison registry; the other two
are deliberate scoping exclusions, not gaps). Also in this group (same
"never parsed, presence checked via `Path.exists()` only" treatment, but no
scan since they aren't CSVs): `docs/governance/governance_interpretation_guide.md` and
`docs/governance/governance_question_routes.md` — human/LLM-authored static reference
docs checked into the repo, not per-run outputs of this generator, and
unconditional on `--emit-interpretation-layer` (see below).

**Generated artifacts** (8): the three existing outputs
(`governance_domain_summary.csv`, `governance_client_summary.csv`,
`governance_narrative_context.md`) plus the five new artifacts described
above (`governance_package_manifest.json`, `governance_package_health.json`,
`governance_findings.json`, `governance_evidence_map.json`,
`governance_brief.md`), including a self-entry for
`governance_evidence_map.json` itself (`related_artifacts` lists all 26
other artifact IDs). Unlike the other generated artifacts (whose `present`
is asserted `True`, since `build_evidence_map()` only ever runs after
they're already written), `governance_brief.md`'s `present` is a real
`Path.exists()`-style check against whether `--emit-interpretation-layer`
was actually on for this run — it is the one generated artifact in this
phase that can be legitimately absent even though the rest of the package
was generated.

The four `policies/governance/*.json` policy-profile files are **not**
separate evidence-map entries — they are policy *inputs* to classification
(recorded by `profile_id`/`schema_version`/`source` in
`governance_package_manifest.json`'s `policy_profiles.profiles` and in
`governance_package_health.json`'s `policy_load_status`), not per-run
corpus evidence artifacts.

Each entry carries `artifact_id`, `path`, `artifact_type`, `required`,
`producer`, `authority_level`, `context_role`, `grain`, `key_fields`,
`identifiers`, `join_keys`, `can_answer`, `cannot_answer`,
`known_limitations`, `null_semantics`, and `related_artifacts` — matching
the candidate evidence-map field list in
`llm_evidence_framework/discovery/evidence_map_discovery.md`.

The evidence map's own artifact count has grown across later phases beyond
the figure above (the relationship layer added three artifacts; D-023 below
adds a 33rd, `governance_file_inventory`; D-024 adds two more —
`pattern_reuse_summary_by_domain` and `project_mean_file_pair_jaccard_matrix`
— bringing the total to 35) — the code (`build_evidence_map()` in
`tools/governance_evidence_package.py`) is the current source of truth for
the exact count and list; see `test_evidence_map_has_thirty_five_unique_artifacts`
in `tests/test_governance_evidence_package.py` for the up-to-date total.

### `governance_file_inventory.json` (D-023)

Authority: `authoritative_deterministic_evidence` (directly observed file
structure, not an interpretation). Built fresh on every run by
`inventory_export_directory_files()`: a `Path.glob("*.csv")` scan of the
cross_segment export directory (`--summary`'s parent) and, when it differs,
the relationship-layer output directory, excluding every path already
tracked as an input, output, or sibling artifact elsewhere in this package.
For each undiscovered file it records the column header, an inferred
per-column dtype (`integer`/`float`/`boolean`/`string`/`empty`), and the
row count — **never a sample row or cell value** ("type of data, not shape
of values"). A short narrative sentence per file is attached by
`generate_governance_narrative.py`'s `_narrative_for_inventory_entry()`:
when the filename matches a `matrix_name` already documented in
`matrix_output_manifest.csv` (if supplied), it reuses that row's own
`interpretation`/`known_limitations` text verbatim — the same free-text
narrative pattern `compare_cross_segment.py`'s `add_manifest()` already
uses for the registered `project_*` matrix artifacts, just applied to a
matrix this generator hasn't wired a CLI flag for yet; otherwise it falls
back to a structural sentence built only from the header/row-count this
scan already computed. `related_artifacts` is intentionally empty — unlike
every other entry, the files this artifact lists vary run to run.

`governance_brief.md` renders the same already-scanned data (no second
scan — `render_governance_brief()` is passed the built
`governance_file_inventory.json` document and only renders it) as its own
`## Detail-Layer File Inventory` section, appended after the leadership
questions and omitted entirely (not blank-rendered) when the scan found
nothing undiscovered — a directory of what exists at the detail layer,
deliberately not interleaved into the per-domain findings sections above
it. `governance_narrative_context.md` itself is unchanged; this section
lives only in `governance_brief.md`, gated by
`--emit-evidence-package`/`--emit-interpretation-layer` the same way the
rest of the brief already is.

**Why this exists:** the package previously described only what it already
knew how to read. A reader (human or LLM) had no way to learn that, say,
`pattern_reuse_summary_by_domain.csv` exists on disk at all — this
generator's own code comments note it is "deliberately not consumed," but
that note lived in Python, not in any artifact a reader could see. This
closes that gap without adding any query/fetch/tool-calling mechanism —
the package remains a single-shot deterministic artifact set; naming a
candidate file is not the same as being able to fetch it.

### Escalation-target sibling artifacts get real shape (D-024)

`docs/governance/governance_interpretation_guide.md`'s "What to do when a pre-built
route isn't enough" section names `cross_segment_file_pairs.csv` and
`comparison_registry.csv` by filename as the drill-down files a route's
"Escalation" field points past the compact layer into. Before D-024, the
evidence-map entries for those two files carried only a hand-written
`context_role`/`can_answer`/`cannot_answer` — real structural facts (the
column header, row count) were never recorded anywhere, so an escalating
reader had to open a multi-GB file cold to learn its schema, or fall back to
`governance_file_inventory.json`'s generic bucket, which only covers files
with no artifact_id at all.

D-024 confirmed, against `generate_governance_narrative.py`'s own module
docstring (not assumed), that the generator's complete "not yet consumed
directly" list is exactly four files: the two named above, plus
`pattern_reuse_summary_by_domain.csv` and
`project_mean_file_pair_jaccard_matrix.csv` (both deliberate scoping
exclusions per `docs/governance_generator_cross_compare_coverage.md`, not
gaps). All four are now registered as `sibling_paths` beside
`cross_segment_summary.csv`'s directory (same inference as `cross_segment_
file_pairs`/`comparison_registry` already used) and get their `columns`/
`row_count` populated by `_sibling_scan_fields()` — a thin wrapper reusing
D-023's `_scan_csv_file()`, not a second scanning implementation. Registering
the two new files as `sibling_paths` also removes them from
`governance_file_inventory.json`'s generic scan bucket (they are no longer
"undiscovered"), so each file gets exactly one narrative home — its own
`can_answer`/`cannot_answer` entry — rather than two competing descriptions
of the same file.

The net effect for a reader following the interpretation guide's escalation
steps: step 1 ("name which large source file is needed") can now cite the
real column names and row count straight from `governance_evidence_map.json`
before writing the filtered extraction script step 2 calls for, instead of
guessing at an unopened file's shape from its filename alone.

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

Implemented (D-021; extended D-029). Governance thresholds, domain-governance
policy, client-onboarding interpretation rules, and anomaly/note materiality
thresholds that used to be Python literals in
`generate_governance_narrative.py` now live in five JSON profiles under
`policies/governance/`, loaded via a new sibling module,
`tools/governance_policy.py`:

| Profile file | Contents |
|---|---|
| `governance_thresholds.json` | Reliability-band cutoffs (`score_reliability()`), tier-assignment bands (`assign_tier()`), cross-client convergence/low thresholds, client alignment/coherence/confidence-band thresholds. |
| `domain_governance_policy.json` | `excluded_from_scoring` (domains excluded from aggregate scoring), `passive_inheritance_risk_domains`, per-domain `domain_guidance` text (`phases`, `loaded_family_types`), and `static_findings_guidance` (always-rendered findings-section prose). |
| `client_onboarding_policy.json` | `_client_onboarding_profile()`'s interpretation thresholds. Kept as its own profile, separate from `governance_thresholds.json`, even where a value numerically coincides with a governance-tier threshold today — these gate onboarding narrative text, not `governance_tier`, and the two are allowed to diverge independently in a future change. |
| `finding_rules.json` | Documentation-only `rule_id → {finding_type, description}` metadata for D-020's `governance_findings.json` `rule_ids[]`. Never drives classification logic — the rule_id constants and the classification rules themselves stay in `generate_governance_narrative.py`. |
| `anomaly_thresholds.json` (D-029) | `detect_anomalies()`'s `notable_anomalies` materiality thresholds, the phases check shared by `detect_anomalies()` and `render_findings_and_recommendations()`, `_passive_inheritance_risk_domains()`'s bundle-share threshold, and `_shape_note()`'s Project Portfolio density-similarity thresholds. Kept as its own profile even where a value numerically coincides with `governance_thresholds.json` (e.g. `passive_inheritance_risk_bundle_share_max` vs. `passive_material_threshold`) — the two gate different code paths and must be independently editable. See `docs/governance/governance_classification_rules.md` for the branch order these values are evaluated in. |

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

## Interpretation guide, question routes, and governance brief

Implemented (D-022). This is the "interpretation and routing split" phase:
it does not change any classification output — `governance_brief.md` is a
pure distillation of `governance_findings.json`/`governance_package_health.json`,
computing nothing new.

### `docs/governance/governance_interpretation_guide.md`

A **stable, package-type-level** document (not regenerated per run) that
explains what this package's metrics and classifications mean: cascade
field semantics, what `governance_tier`/`score_reliability` do and don't
support, comparability rules (sector, unit system, all-view/used-view),
missing-value semantics, authority ordering, and a "known bad inferences"
section. Modeled on the "interpretation layer" concept in
`GMcDowellJr/llm_evidence_framework`'s `notes/current_thesis.md` and
`patterns/deterministic_to_llm_boundary.md` — design reference only, no
runtime dependency. Versioned via a header (`interpretation_guide_version`)
independently of `PACKAGE_SCHEMA_VERSION`.

### `docs/governance/governance_question_routes.md`

A **candidate question-route catalog** — "where to look," not "what the
answer is" (that's the artifact) or "how to extract it" (that would be a
script recipe, not built in this phase). Follows the discovery scaffold in
`llm_evidence_framework/discovery/question_route_discovery.md` (Status /
Question forms / Intent / Primary+Secondary artifacts / Relevant fields /
Evidence type / Supported+Unsupported conclusion types / Comparability
requirements / Common traps / Escalation). Every route in this file is at
**candidate** maturity (that framework's own maturity scale runs candidate →
active → recipe-backed → extractor-backed) — none has a proven history of
repeated use for this package type yet. Seeded from questions this
generator already treats as recurring (the leadership questions rendered in
`governance_narrative_context.md`, and the ten `governance_findings.json`
finding types), not invented from nothing, per that framework's own
"a route should not be codified just because it was imagined" guidance.

### `governance_brief.md`

A **narrower, run-specific** digest — the actual new *generated* artifact in
this phase. Built by `render_governance_brief()`, which consumes the
already-computed `findings` list and `governance_package_health.json`
directly (no new classification logic, matching PR2's "consume, not
recompute" discipline): package status, corpus counts, each finding
category capped at 10–15 items with a pointer to `governance_findings.json`
for the full list, and the leadership questions as a numbered list (marked
distinctly from findings, since they carry `status: question_not_claim`).
Its own header states `authority_level: convenience_summary`, subordinate
to `governance_package_health.json`, the source CSVs, the rollup CSVs, and
`governance_findings.json`.

**`--emit-interpretation-layer` / `--no-emit-interpretation-layer`**
(default: on) controls `governance_brief.md` only, independently of
`--emit-evidence-package` — but only takes effect when
`--emit-evidence-package` is also on, since the brief is built from
`governance_findings.json`/`governance_package_health.json`. The two static
docs are unaffected by either flag; they always appear in the evidence map
(with real `Path.exists()`-based presence) since they are checked-in repo
docs, not per-run outputs. A stale `governance_brief.md` from a prior run is
removed if either flag is turned off between runs over the same `--out`
directory, matching the existing staleness-prevention convention for the
other evidence-package JSON files.

`governance_narrative_context.md`'s authority header now also points to
`governance_brief.md` (when present), `docs/governance/governance_interpretation_guide.md`,
and `docs/governance/governance_question_routes.md`.

## CLI reference

```text
--summary CROSS_SEGMENT_SUMMARY_CSV      (required, unchanged)
--pooled CROSS_SEGMENT_POOLED_CSV        (required, unchanged)
... (all existing optional flags, unchanged) ...
--policy-dir DIR                         optional; default policies/governance/
                                          (see "Policy profiles" above)
--package-schema-version VERSION         optional; default 1.0
--emit-evidence-package                  default: on
--no-emit-evidence-package               suppresses the 4 new JSON outputs
                                          (+ governance_brief.md) only;
                                          existing CSV/MD outputs are unaffected
--emit-interpretation-layer              default: on; governance_brief.md only
--no-emit-interpretation-layer           suppresses governance_brief.md only;
                                          manifest/health/evidence-map/findings
                                          and the static interpretation-guide/
                                          question-routes evidence-map entries
                                          are unaffected
```

No existing invocation needs to change — `--emit-evidence-package` and
`--emit-interpretation-layer` both default to on, so every existing caller
starts producing the five new files (manifest, health, evidence map,
findings, brief) with no CLI change required, and `--policy-dir` defaults
to the shipped `policies/governance/` profiles, which reproduce
pre-externalization output exactly. Pass `--no-emit-evidence-package` to
opt out of the JSON/brief outputs entirely, `--no-emit-interpretation-layer`
to opt out of just `governance_brief.md`, or `--policy-dir` to point at a
different (or partial, or nonexistent) policy profile set.

## Recommended LLM navigation

A human or LLM analyzing a governance package produced by this generator
should normally load, in order — this mirrors the "context-budget pattern"
in `llm_evidence_framework/notes/current_thesis.md` (reasoning guidance +
package-specific interpretation guidance + manifest + health + evidence map
+ rollup/flags + the user's question, not the full archive):

1. `docs/governance/governance_interpretation_guide.md` — what do this package's
   metrics/tiers mean, and what are the known bad inferences to avoid?
   (Static; load once per package type, not per run.)
2. `governance_package_health.json` — is this package usable at face value?
   (includes `policy_load_status` — did every policy profile actually load,
   or is a profile running on this generator's built-in default?)
3. `governance_evidence_map.json` — which artifact answers the question at
   hand? If the question matches a recurring pattern, check
   `docs/governance/governance_question_routes.md` first for a candidate shortcut.
4. `governance_package_manifest.json` — what inputs actually fed this run,
   and which `policy_profiles.profiles` (`profile_id`/`schema_version`/
   `source`) were applied?
5. `governance_brief.md` for a fast top-line read of this run's findings, or
   `governance_findings.json` directly for the full, structured list —
   which domains/clients meet a specific named rule, and what CSV rows/
   fields support that classification.
6. Relevant rows from `governance_domain_summary.csv` / `governance_client_summary.csv`.
7. `governance_narrative_context.md` only for framing/prose context — never
   as the sole source for a claim that a CSV, `governance_findings.json`, or
   `governance_brief.md` can verify.
8. If the question needs data none of the above can answer,
   `governance_file_inventory.json` (or `governance_brief.md`'s "Detail-Layer
   File Inventory" section) before giving up — it names candidate files this
   package doesn't otherwise describe, with real header/row-count, even
   though this generator cannot fetch or parse them itself.

The full evidence archive (source comparison CSVs, sibling
`cross_segment_file_pairs.csv`/`comparison_registry.csv`, and any file named
in `governance_file_inventory.json`) should be pulled only when a question
requires drill-down or verification beyond what the rollup CSVs and evidence
map already answer.

## Enterprise-policy provenance

Identity-aware packages accompany their primary artifacts with
`enterprise_policy.json`. The canonical sorted UTF-8 payload records the policy
schema, effective enterprise label, enterprise BC token, configuration source,
and a content-derived configuration identifier; it excludes absolute local
paths. Governance-manifest, cross-segment comparison, relationship, narrative,
and promotion-analysis producers publish provenance only after validation and
primary output generation. Comparison dry runs and source/policy validation
failures do not create it. A generation failure before publication therefore
cannot leave new provenance that misdescribes a partial package.

Promotion-analysis CSV schema v2 uses
`reuse_client_pool_is_enterprise`. The former organization-specific field has
no compatibility alias; pre-v2 consumers must rename it explicitly. This flag
uses the effective EnterprisePolicy label and does not infer enterprise status
from the `0000` bookkeeping BC token. The analysis still has no BC dimension in
its reuse grain, so this migration does not claim broader reuse precision.
