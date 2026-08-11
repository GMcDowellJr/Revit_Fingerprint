# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Revit Fingerprint** extracts deterministic, behavior-based fingerprints from Revit models. It identifies **what a model does**, not how it is named or presented in the UI. The system enables standards governance, drift detection, and cross-project comparison.

**Primary runtime**: Dynamo CPython3 (via `runner/run_dynamo.py`)

The repo has two halves that share almost no runtime dependency:
- **Extraction** (`core/`, `domains/`, `runner/`, `validators/`) — runs inside Revit/Dynamo, produces `*.details.json` / `*.index.json` exports.
- **Analysis** (`tools/`) — runs on a developer machine against exported JSON/CSV, has no Revit dependency, and is where most active development currently happens (see `tools/` below).

## Commands

```bash
# Run all unit tests
pytest tests/ -v

# Run a single test file
pytest tests/test_hashing_incremental.py

# Validate exported JSON against the record.v2 contract
FINGERPRINT_JSON_PATH=/path/to/export.json pytest tests/test_record_contract_v2.py
```

No `requirements.txt` or `pyproject.toml` exists. The only external dependency for development is `pytest` (`pip install pytest`). `.github/workflows/ci.yml` runs `pytest tests/ -v` on Python 3.9–3.12 for every push/PR to `main`. A second workflow, `.github/workflows/graphify.yml`, keeps the `graphify-out/` knowledge graph in sync.

Analysis tools are stdlib-only (no pandas/numpy) as a rule — `tools/` code reads/writes CSV via `csv.DictReader`/`csv.DictWriter` by convention; don't introduce a new dependency there without a strong reason. Known exceptions that already require `pandas`/`numpy`/`scipy`: `tools/patterns_analysis/split_detection.py`, `split_detection_file_level.py`, `split_detection_element_level.py`, `tools/pareto_joinkey_search.py`, and `tools/analyze_promotion_candidates.py` (inherited pandas dependency from the prototype it redesigns).

## Architecture

The extraction system is domain-driven and layered:

```
Layer 0 - Core (Pure Python)     → core/
Layer 1 - Domain Extractors      → domains/
Layer 2 - Context Builder        → core/context.py, core/collect.py
Layer 3 - Host-specific Runners  → runner/
```

**Dependency direction**: `Core → Domains → Context → Runner`

Reverse dependencies are forbidden. Domains do NOT import each other.

`tools/` (analysis) sits downstream of exported JSON and is architecturally separate — it does not import from `domains/`/`runner/`, and extraction code must never import from `tools/`.

## Directory Structure

```
core/                   Pure Python utilities (no Revit API calls)
  hashing.py            MD5 hashing (CLR/hashlib dual-runtime)
  canon.py              Canonicalization + sentinels (<MISSING>, <UNREADABLE>, <NOT_APPLICABLE>)
  contracts.py          Contract envelopes, status rollups, bounded errors
  record_v2.py          record.v2 schema utilities & canonicalization
  phase2.py             Phase-2 join-key/join-hash helpers (semantic/cosmetic/coordination/unknown buckets)
  canonical_items.py    Canonical flat `items:[{k,v,q}]` helpers — migration path off the phase2 bucket shape
  sig_hash_policy.py    Loads/validates policies/domain_sig_hash_policies.json
  sig_hash_builder.py   Policy-driven sig_hash computation from flat items (analysis-side; see below)
  context.py            View-scoped context (ViewInfo, DocViewContext)
  deps.py               Dependency enforcement (Blocked exception, require_domain)
  rows.py               Parameter reading, unit conversion
  collect.py             FilteredElementCollector caching
  join_key_builder.py   Build join keys from policies with shape-gating
  join_key_policy.py    Load & validate join-key policies
  name_key_builder.py   Analysis-side reconstruction of the Canonical Name Identity Projection
                            (join_key_name_identity) from already-exported record.v2 JSON — mirrors
                            sig_hash_builder.py's role for sig_hash
  name_key_coverage.py  Native/Widened/Excluded coverage-class registry for the Canonical Name
                            Identity Projection; single source of truth, see tools/generate_name_key_patterns.py
  graphic_overrides.py  Shared helpers for graphics extraction
  features.py           Cohort-analysis feature surface
  naming.py             Document-derived naming helpers
  manifest.py            Stable manifest surface for comparison
  dimension_type_helpers.py  Shape constants, detection, and reading helpers (shared by dimension_types)
  timing_collector.py   Extraction profiling instrumentation
  vg_sig.py              VG signature helpers for view_templates

domains/                One extract(doc, ctx) function per domain (active)
  identity.py            Project metadata; despite the module's historical "no hash" framing, it DOES
                            compute a real sig_hash (D-025) from a subset of captured fields including
                            project_info.* — see the module's own top-of-file note
  units.py                Length/area/volume format options
  object_styles.py        Object style definitions (model/annotation/analytical/imported partitions)
  line_patterns.py        Line pattern definitions (scale-invariant normalized-segment join key, D-017)
  line_styles.py          Line style definitions
  fill_patterns.py        Fill pattern definitions (drafting/model partitions)
  text_types.py            Text type definitions (piloting canonical flat `items` shape via core/canonical_items.py)
  arrowheads.py            Arrowhead definitions with shape-gating
  dimension_types.py      Dimension type definitions (7 partitions: linear/angular/radial/diameter/
                            spot_elevation/spot_coordinate/spot_slope)
  phases.py                Phase inventory & sequence
  phase_filters.py        Phase filter definitions
  phase_graphics.py       Phase graphic overrides (DISABLED - API limitation, D-013)
  view_filter_definitions.py        Detailed filter rule extraction
  view_filter_applications_view_templates.py  Filter application stacks
  view_templates.py       Template definitions (5 partitions by ViewType family)
  view_category_overrides.py        VCO coordinator
  view_category_overrides_model.py      Model category override partition
  view_category_overrides_annotation.py Annotation category override partition
  materials.py             Materials domain (identity + graphics state; also populates ctx lookup maps)
  wall_types.py            Compound type family: wall_types partition (active)
  floor_types.py           Compound type family: floor_types partition (active)
  roof_types.py            Compound type family: roof_types partition (active)
  ceiling_types.py         Compound type family: ceiling_types partition (active). D-018's remaining
                            compound_types gap is other system-family types (stacked walls, curtain walls,
                            curtain systems, MEP system types), not these four partitions
  compound_layers.py       Shared compound-structure-layer helpers for wall/floor/roof/ceiling_types;
                            not a domain extractor itself (no extract() entry point)
  loaded_family_types.py  FamilySymbol (loaded family) types, parameter-schema evidence model (lft.*/lftp.*); scoped
                            to user-loaded families only — system families pass through but aren't governed (D-018)
  worksets.py              Workset partition (worksets + worksets_doc, two independent single-collector extractors)
  browser_organization.py Project Browser sorting/filter-presence configuration (category, sorting_order,
                            sorting_parameter_id, filter_has_value); consumes worksets' ctx crosswalk. NOT
                            grouping coverage — the GetFolderItems folder hierarchy is deliberately excluded
                            (non-deterministic probe-run-dependent tree walk), so two organizations differing
                            only in grouping can produce the same sig_hash; see the module's own docstring
  graph_2024.json, graph_2025.json, graph_2026.json
                            NOT extractor code — cached Revit-API relationship graphs (per Revit version) generated
                            by sync_revitlookup_reference.py / the RevitLookup sync tooling, used as reference data
                            for REVIT_LOOKUP_DOMAIN_MAP.md. Do not import these from a domain extractor.

runner/                 Host-specific entry points
  run_dynamo.py          Primary Dynamo CPython3 runner
  thin_runner.py          Lightweight wrapper for Dynamo environment control
  probe_thin_runner.py    Probe-mode variant of the thin runner
  purge_sys_modules_standalone.py  Clears cached Revit-side module imports between runs (Dynamo re-run hygiene)

validators/             Output validation
  record_v2.py           record.v2 schema validation

policies/               Join-key, sig-hash, and governance-classification policies
  domain_join_key_policies.json     Per-domain join-key policies with shape-gating
  domain_sig_hash_policies.json     Per-domain sig_hash policy (generated from contracts/domain_identity_keys_v2.json
                                       via tools/generate_sig_hash_policy.py); consumed by core/sig_hash_builder.py
  domain_name_key_policies.json     Per-domain policy for the Canonical Name Identity Projection
                                       (join_key_name_identity) — same loader/mechanism as
                                       domain_join_key_policies.json (core/join_key_policy.py), independent
                                       policy file; runner loads it into ctx["name_key_policies"]; analysis-side
                                       consumers are core/name_key_builder.py / tools/apply_name_key_policy.py
  cross_domain_alignment_keys.json  Domain family registry and alignment key definitions
  governance_role_path_patterns.json  Ordered path-substring rules that infer governance_role
                                       (Template/Container/Project/Generic) from central_path_norm
  placeholder_known_defaults.json   Per-domain known-default/placeholder name patterns used by the `placeholders` stage
  client_sector.csv                 client_label → sector classification, used by generate_governance_narrative.py
  governance/                       Externalized governance-narrative policy profiles, loaded via
                                       tools/governance_policy.py (D-021): governance_thresholds.json (tier/
                                       reliability/convergence/coherence thresholds), domain_governance_policy.json
                                       (excluded_from_scoring, passive_inheritance_risk_domains, domain guidance
                                       text), client_onboarding_policy.json (onboarding-interpretation thresholds),
                                       finding_rules.json (rule_id → finding_type/description documentation).
                                       Shipped values reproduce generate_governance_narrative.py's
                                       pre-externalization Python literals exactly -- see
                                       docs/governance_evidence_package.md.

config/
  archetype/archetype_definitions.json   Human-curated (DP1) archetype definitions consumed by archetype tooling
  archetype/static_edges_seed.json        Seed edges for archetype signal graph construction

reference/
  revit_lookup/Descriptors/*.cs     RevitLookup C# descriptor source, synced via sync_revitlookup_reference.py
                                       (root script; GitHub API fetch, no git clone). Ground truth for what the
                                       Revit API actually exposes per type — cross-reference when auditing an
                                       extractor; see REVIT_LOOKUP_DOMAIN_MAP.md for the domain → descriptor map.

tools/                  Analysis & comparison utilities (no Revit dependency; stdlib CSV/JSON only)
  run_extract_all.py     Primary orchestrator — explicit stage machine (see Analysis Pipeline below)
  extractor.py            Engine behind the `patterns`/`authority` stages — `run_extract_all.py` imports
                            `emit_analysis`/`emit_records` from this module directly. Owns the production
                            join_hash-based pattern-clustering algorithm (`_stable_pattern_id()`), path
                            normalization (see `docs/CENTRAL_PATH_NORM_RULE.md`), and export-file discovery.
  pattern_id_utils.py     Shared `pattern_id`/`pattern_label` formula per `docs/PATTERN_ID_AND_LABEL_RULES.md`;
                            a deliberately independent reimplementation of `extractor.py`'s private
                            `_stable_pattern_id()` (kept stdlib-only, not imported from `extractor.py`, so
                            `generate_name_key_patterns.py` can't accidentally couple to the production
                            pipeline). No test currently cross-checks the two implementations stay in
                            agreement — `tests/test_generate_name_key_patterns.py` only calls the public
                            `pattern_id_utils.stable_pattern_id()`, never `extractor._stable_pattern_id()` —
                            so a future change to only one would go uncaught; a real regression test is TODO.
  run_config.json        Phase-1 configuration (domains_in_scope, thresholds, seed_baseline_id)

  export_to_flat_tables.py   Phase-0: Flatten record.v2 details → CSV tables (records, identity_items, etc.)
  discover_join_policy.py / apply_join_policy.py   Join-key policy discovery/apply (T1/T2 stages)
  discover_hash_policy.py / generate_sig_hash_policy.py   sig_hash policy discovery/generation (see below)
  generate_name_key_patterns.py / apply_name_key_policy.py   Canonical Name Identity Projection (PR1/PR2):
                            apply_name_key_policy.py computes join_key_name_identity per record from
                            already-exported *.details.json (via core/name_key_builder.py, no re-extraction
                            needed); generate_name_key_patterns.py parameterizes pattern generation over
                            `--comparison-target {config,name,both}` so it can run against either the
                            existing join_hash or the name-identity projection
  join_key_discovery/     Greedy/scored join-key candidate search shared by discover_join_policy.py and
                            discover_hash_policy.py (eval.py, greedy.py)
  join_key_derivation.py, compute_governance_thresholds.py, compute_latent_purgeable.py

  domain_authority.py, population_framing.py, pairwise_analysis.py
                          Phase-1 style authority/coverage/pairwise summaries (formerly `phase1_*.py`)

  bundle_analysis/        Placeholder/bundle pipeline: step0 (discover populations) → step1 (membership matrix)
                            → step2/2b (find bundles, share profile) → step3 (DAG) → step4 (difference sets)
                            → step5/6 (classify patterns/files) → step7 (overlap report). run_bundle_analysis.py
                            drives the full sequence; placeholder_exclusions.py implements the placeholder heuristic.
  patterns_analysis/       Split-detection analysis (file-level and element-level)
    _archive/              NOT confirmed-dead despite the name — the old tools/phase2_analysis/ package was moved
                            here wholesale. Most of it is still live: run_split_detection_all.py invokes 9 of its
                            modules directly (split_detection_file_level, build_reference_standards,
                            intradomain_summary, emit_intradomain_definition, derive_join_keys_by_ids,
                            apply_join_keys_by_ids, calibrate_join_key_gates, pareto_join_keys_by_ids,
                            split_detection_element_level), and tests import _archive.io directly. The unreferenced
                            remainder (run_change_type.py, run_attribute_stress*.py, etc.) is intentionally-paused
                            Phase-2-baseline tooling (see "Two distinct baseline concepts" below), not dead code —
                            do not delete without re-verifying against live call sites first.
  label_synthesis/         Label synthesis / fragmentation repair for domain patterns (build_label_population.py,
                            synthesize_fragmented_labels.py, domain_prompts/, synopsis_formatters/)
  probes/                  ~25 domain-specific Revit API probe scripts + PROBE_INVENTORY.md/.csv (measure-first
                            inputs to domain design decisions)
  migration/               One-off/point-in-time data migration scripts (reformat_to_flat_items.py,
                            migrate_materials_identity_items.py, compress_fingerprint_json.py)
  lib/                     Shared library code for tools/ (diff_engine.py, domain_profile.py, vt_profile.py)

  build_segment_manifest.py     Build the corpus segmentation lattice (segment_manifest.csv / registry / membership)
                                  from file_metadata.csv — every subset of (unit_system, governance_role,
                                  client_label, discipline_label, business_center_label, collection_label)
  run_segment_orchestrator.py   Reads segment_manifest.csv + run_registry.csv; runs patterns → bundle_analysis
                                  stages per segment in level order; writes per-segment output folders
  extract_segment_subtree.py    Pulls one segment + its ancestors' cross_segment_*.csv rows into a standalone subset
  build_results_registry.py     Builds BI-friendly results_registry.csv from segment_manifest.csv + run_registry.csv
                                  — one row per segment, a single stable query surface instead of hand-wiring
                                  individual segment output folders
  export_bundle_pattern_detail.py   Exports the bundle -> pattern -> identity_items -> name_population chain for
                                  one segment as 3 flat BI CSVs (bundle_pattern_inventory/pattern_settings/pattern_names)
  emit_element_dominance.py     Element-level dominance/BI export utility
  inspect_lft_similarity.py     Single-pass loaded_family_types similarity analysis across a container corpus,
                                  grouped by sig_hash, with optional dimensional/classification enrichment
  reset_wall_types_for_reapply.py   Resets wall_type records blocked solely on wt.function=unsupported.not_applicable
                                  so the apply stage can re-evaluate them
  governance_manifest.py        Builds disjoint governance populations (Enterprise / each business center /
                                  each client / each named project / Generic) directly from file_metadata.csv —
                                  deliberately NOT built on the segment lattice's powerset (see file docstring)
  compare_cross_segment.py      Cross-segment comparison using join_hash as the identity unit (Jaccard/containment);
                                  supersedes tools/similarity_compare.py (deprecated in place, not archived — see
                                  docs/tools_DEPRECATED.md for the specific correctness bugs that motivated this)
  compare_governance_populations.py   Same containment/Jaccard mechanics as compare_cross_segment.py, applied to
                                  governance_manifest.py's disjoint populations (imports rather than reimplements)
  generate_governance_narrative.py    Deterministic (no-LLM) governance_narrative_context.md renderer from the
                                  compare_cross_segment.py / bundle pipeline CSV outputs; also emits a governance
                                  evidence-package layer (governance_package_manifest.json/_health.json/
                                  _evidence_map.json/_findings.json/governance_brief.md) via
                                  governance_evidence_package.py, loads governance thresholds/domain policy/
                                  onboarding policy from policies/governance/*.json via governance_policy.py,
                                  and points readers at docs/governance_interpretation_guide.md /
                                  docs/governance_question_routes.md -- see docs/governance_evidence_package.md
                                  and D-019/D-020/D-021/D-022/D-023/D-024. Also emits
                                  governance_file_inventory.json (D-023/D-024): a live Path.glob("*.csv") scan of
                                  the export directories that names files this generator never reads (header,
                                  inferred dtype, row count only -- never sample rows/cell values), so an escalating
                                  reader can discover a drill-down file exists before writing an extraction script
                                  against it.
  governance_evidence_package.py    Package manifest/health/evidence-map/findings/file-inventory builders for the
                                  governance narrative evidence package (see docs/governance_evidence_package.md).
                                  Design-reference-only relationship to the external
                                  GMcDowellJr/llm_evidence_framework repo -- no import from or runtime dependency
                                  on it.
  governance_policy.py            Generic JSON policy-profile loader (mechanical load/fallback only) for
                                  policies/governance/*.json, used by generate_governance_narrative.py. Owns no
                                  governance business content itself -- default threshold values and
                                  domain-governance logic stay in generate_governance_narrative.py (D-021).
  governance_relationships.py     Relationship/topology evidence layer: project-level composition (which
                                  projects belong to which client/business-center, file counts) and BC<->client
                                  rollups derived from file_metadata.csv. Presentation/aggregation only, not
                                  new derivation logic; does not read cross_segment_summary.csv's
                                  cascade-producing comparison types.
  governance/standards_governance_report.py   Standards governance report generator

  archetype/               Archetype candidate generation & DP1 (Decision Point 1) human-curation workflow:
                            discover_vfd_edges.py → build_cross_domain_items.py → compute_cross_domain_cooccurrence.py
                            → cluster_archetype_signals.py → generate_archetype_candidates.py → human review
                            (review/) → assign_archetype_classifications.py; validated against
                            config/archetype/archetype_definitions.json
  compare_templates_stand-alone/   Standalone view-template comparison tool + HTML report (independent of the
                            segment/governance pipeline above)
  analyze_promotion_candidates.py   Standalone scope-consistency classifier: reads cross_segment_governance_states.csv
                            + pattern_reuse_distribution.csv and flags patterns whose observed reuse scope
                            (`reuse_scope`, from `reuse_bucket`) exceeds the broadest scope at which they are
                            already governed by a Template/Container (`seeded_scope`, from directed
                            comparison_type edges). Descriptive scope-consistency classification, not a
                            promotion decision -- not pipeline-wired, not rendered by
                            generate_governance_narrative.py, no assign_tier() interaction. reuse_scope can
                            never resolve to bc-level (pattern_reuse_distribution.csv's grouping key has no
                            business_center_label dimension) -- see the module docstring for the full list of
                            known upstream measurement gaps this tool works around rather than papers over.

  na_token.py             Shared "N/A"-spelling detection used by segment/governance tooling
  jenks_utils.py           Jenks natural-breaks helper for threshold computation
  pareto_joinkey_search.py  Pareto-front join-key search; backs discover_join_policy.py's pareto/"harsh" mode
                            and tests/test_pareto_shape_gating.py. Known issue: throws `KeyError: 'max_sigcnt'`
                            under pandas on at least minimal synthetic inputs — pre-existing algorithm bug,
                            not a location/import problem (see docs/tools_DEPRECATED.md).
  run_split_detection_all.py   Split detection over all domains
  acc_scan_dc.py / acc_sync_dc.py   Corpus-collection operator tools (ACC Desktop Connector): scan_dc walks a
                            root folder and writes an include-flagged manifest CSV of Revit files to fingerprint;
                            sync_dc hydrates online-only Desktop Connector stub files before the BatchExtract
                            pyRevit run consumes them
  Powershell Commands.txt  Informal operator runbook (hardcoded paths) — closest thing to a runbook; not automated

  _archive/                Confirmed-superseded tools, pruned down to only what's still load-bearing:
                            join_key_derivation_phase05.py (still `import *`-ed live by join_key_derivation.py).
                            Everything else with zero remaining references (compare_manifest.py, merge_split_exports.py,
                            score_drift.py, pairwise_drift.py, validate_v21_contract.py, etc.) was deleted outright —
                            see docs/tools_DEPRECATED.md for the full list and the 2026-07-16 cleanup note.
                            Do not build new work on top of anything here without checking CHANGELOG.md first.

tests/                  pytest test suite (70+ test files)
  test_sentinel_policy.py            Enforce only 3 allowed sentinels
  test_hashing_incremental.py        Hash determinism
  test_contracts_run_status.py       Status rollup (failed > degraded > ok)
  test_contracts_bounded_errors.py   Bounded error handling
  test_record_contract_v2.py         record.v2 schema validation
  test_record_v2_utils.py            record.v2 utility tests
  test_no_direct_filtered_element_collector_in_domains.py  Architecture enforcement
  test_deps_require_domain.py        Dependency blocking
  test_arrowheads_shape_gating.py / test_dimension_types_shape_gating.py / test_pareto_shape_gating.py
  test_join_key_policy_validation.py / test_join_key_builder_shape_gating_dedupe.py / test_join_key_migration.py
  test_sig_hash_policy_builder.py    Sig-hash policy builder tests
  test_canonical_items_migration.py  core/canonical_items.py flat-items migration tests
  test_split_export.py, test_record_id_determinism.py, test_timing_collector.py, test_graphic_overrides.py, test_collect.py
  test_*_canonical_selectors.py      Domain-specific canonical selector tests (one per active domain)
  test_build_segment_manifest.py, test_compare_cross_segment_*.py, test_compare_governance_populations.py,
  test_governance_manifest.py, test_governance_field_completeness_gate.py, test_generate_governance_narrative_*.py,
  test_run_segment_orchestrator_worker_split.py, test_bundle_pattern_classification_roles.py,
  test_placeholder_exclusions.py, test_reference_bundle.py, test_label_synthesis_domain_prompt_loader.py,
  test_discover_hash_policy.py, test_discover_vfd_edges.py, test_na_token.py, test_probe_inventory_builder.py,
  test_analyze_promotion_candidates.py  Synthetic-fixture coverage for tools/analyze_promotion_candidates.py
                                       (scope-gap routing, ordinal ranking, no-bare-score invariant)
                                       Coverage for the newer segment/governance/archetype tooling
  revit/                              Revit integration harness (requires Revit)
  golden/                             Golden file comparisons

contracts/              Machine-readable contracts
  record_contract_v2.md              record.v2 schema documentation
  record_contract_v2.schema.json     JSON schema for validation
  domain_identity_keys_v2.json       Per-domain key registry with minima — source of truth generate_sig_hash_policy.py
                                       compiles into policies/domain_sig_hash_policies.json
  phase2_join_keys.md                Phase-2 join key specification

docs/                   Technical documentation
  join_key_shape_gating.md           Shape-gating schema extension
  SPLIT_EXPORT.md                    Split export data model
  phase2-identity-and-semantic-plan.md  Phase-2 contract design
  phase_2_join-key_discovery.md      Join-key discovery methodology
  fingerprint_hashing_rules.md       Hashing rule documentation
  hash_discovery_tooling.md          discover_hash_policy.py / generate_sig_hash_policy.py usage
  extract_stage_matrix.md            Authoritative stage-machine reference for run_extract_all.py (current)
  CSV_CONTRACT_v2.1.md               v2.1 CSV output contract
  V21_PHASE0_EXPORT_SCHEMA.md        v2.1 Phase-0 export schema
  V21_ANALYSIS_SCHEMA.md             v2.1 analysis output schema (Results_v21/analysis_v21/)
  V21_DETERMINISM_AND_IDENTITY.md    v2.1 determinism rules + pattern-id derivation
  CENTRAL_PATH_NORM_RULE.md          Canonical file-path normalization rule (feeds governance_role inference)
  PATTERN_ID_AND_LABEL_RULES.md      Pattern id / label derivation rules
  cross_segment_comparison.md        compare_cross_segment.py methodology
  governance_narrative_scope_gap_audit.md   Known gaps in governance narrative scope coverage
  governance_narrative_group1_scope_gap_investigation.md   Historical investigation of a Group 1
                                       (tc/cp/tp) scope-gating gap in build_cascade() — findings partially
                                       implemented; consult current code/tests for active behavior
  governance_generator_cross_compare_coverage.md   Tracks which compare_cross_segment.py CSV outputs
                                       generate_governance_narrative.py already consumes vs. only partially
                                       covers
  governance_evidence_package.md     generate_governance_narrative.py's evidence-package layer (manifest/
                                       health/evidence-map/findings) — see also "Key docs for analysis work" below
  governance_interpretation_guide.md Stable, package-type-level interpretation guide for a governance
                                       evidence package
  governance_question_routes.md      Candidate question-route catalog for recurring governance-package questions
  METRICS.md                         Concentration metric contracts (HHI / effective clusters)
  analysis-phases-question-map.md    Analysis questions mapped to phases
  tools_PHASE0_1_2_MAP.md            ⚠ Dated 2026-01-29; still references a `tools/phase2_analysis/` package
                                       path that no longer exists (superseded by the segment/governance tools
                                       above). Treat as historical context, not a current tool index.
  tools_DEPRECATED.md                ⚠ Same staleness caveat — deprecation reasoning is still valid, but some
                                       "replacement" commands reference the same removed `phase2_analysis/` path.
  phase_2_join_key_discovery_summary   Plain-text summary companion to phase_2_join-key_discovery.md
  method_invocation_candidates_annotated.csv   Annotated shortlist of RevitLookup methods considered for the
                                       probe reflection sweep
  probe_method_invocation_candidates_verification.md   Findings-only Step-0 verification pass over
                                       method_invocation_candidates.csv (arity/staticness/return-type/
                                       mutation-safety); no code changes
  probe_method_invocation_serialization_findings.md   Findings-only follow-up diagnosing serialization-gap/
                                       missing-invocation/ambiguous-quality-state issues in PROBE_INVENTORY.csv
  research/                          RevitLookup API concept-mapping working files

legacy/                 MVP implementation (preserved reference)
  fingerprint_mvp.py
```

## Critical Rules (Non-Negotiables)

### Hash Semantics
- Hashes MUST be deterministic, stable across sessions, independent of element creation order
- Hash inputs represent **behavior**, not presentation or naming
- Names are **metadata only** - never included in behavior hashes unless explicitly stated
- **Exception (D-010)**: Phase names ARE included in behavioral hashes for cross-project comparability
- Hashing is semantic-only (`hash_v2`); legacy pipe-delimited mode has been removed (D-014)

### Sentinel Policy (PR3)
Only THREE angle-bracket sentinels are allowed:
- `<MISSING>` — value None/empty/unset
- `<UNREADABLE>` — value unreadable/exception
- `<NOT_APPLICABLE>` — value not applicable to element type

### Fail-Soft Policy
- NEVER silently collapse distinct states
- Unreadable/inaccessible data MUST emit explicit markers
- Errors propagate into hashes intentionally

### record.v2 Schema
Every record MUST have:
- `schema_version: "record.v2"`
- `domain`, `record_id`, `status`, `status_reasons`
- `sig_hash` (null iff status == "blocked")
- `identity_basis` with `items: [{k, q, v}]` format
- `identity_quality`, `label`

Identity values (`v`) MUST NOT contain sentinel literals — use `v: null` + `q: "missing"` instead.

**Identity Quality Dominance** (in order):
`none_blocked` > `incomplete_unreadable` > `incomplete_unsupported` > `incomplete_missing` > `complete`

### UniqueId Usage
Use `UniqueId` ONLY for element-backed entities where identity persistence matters (filters, phases, templates, views). Styles, patterns, and definitions use name-based or composite keys.

### Ordering Rules
- Order-sensitive structures (e.g., view filter stack): preserve order
- Order-insensitive structures: sort before hashing
- Each domain MUST explicitly state its ordering behavior

## Domain Family Architecture (D-015)

Consolidated extractors route records internally by record class (Revit system family boundary). Each partition emits its own `sig_hash` and `domain` label within a shared extractor file.

**Domain family mappings**:

| Domain file | Record-class partitions (emitted domain names) |
|-------------|------------------------------------------------|
| `object_styles.py` | `object_styles_model`, `object_styles_annotation`, `object_styles_analytical`, `object_styles_imported` |
| `fill_patterns.py` | `fill_patterns_drafting`, `fill_patterns_model` |
| `dimension_types.py` | `dimension_types_linear`, `_angular`, `_radial`, `_diameter`, `_spot_elevation`, `_spot_coordinate`, `_spot_slope` |
| `view_templates.py` | `view_templates_floor_structural_area_plans`, `_ceiling_plans`, `_elevations_sections_detail`, `_renderings_drafting`, `_schedules` |
| `view_category_overrides*.py` | Routed via `view_category_overrides.py`; model and annotation in separate files |
| `wall_types.py` / `floor_types.py` / `roof_types.py` / `ceiling_types.py` (+ shared `compound_layers.py` helpers) | All four active. D-018's remaining coverage gap is other system-family types (stacked walls, curtain walls, curtain systems, MEP system types), not these partitions |

For consolidated extractors, `extract()` returns a list of per-partition result dicts, each with its own `domain` key.

**Key vocabulary**:
- **Domain family**: Named grouping (e.g., `object_styles`). Policy and BI concept; no code hierarchy.
- **Domain**: Extractable unit with one policy entry and one `sig_hash`. Partitions within a file are flat peers in the runner.
- **Record class**: Entities within a domain that use different identity properties, routed by class discriminator.

## Phase-2 Buckets

Records partition their items into four buckets:

| Bucket | Purpose | Behavior |
|--------|---------|----------|
| `semantic_items` | Behavior-defining items | Exported for join-key discovery and Phase-2 comparisons |
| `cosmetic_items` | Labels and presentation | Used for pattern detection, excluded from behavior hashes |
| `coordination_items` | Cross-model resolution | Name-based lookups (e.g., ByHost), never in behavior hashes |
| `unknown_items` | File-local noise | UIDs/ElementIds for traceability, excluded from join-keys |

**Phase-2 Invariants**:
- `sig_hash` is authoritative and UID-free by contract
- `identity_basis.items` contains the full behavioral definition and drives `sig_hash`

**Migration in progress**: `core/canonical_items.py` introduces a flatter, role-agnostic shape (`items: [{k, v, q}]`) where the semantic/cosmetic/coordination/unknown role is resolved **at runtime from policy**, not baked into the extracted JSON. `text_types.py` is the pilot domain; `runner/run_dynamo.py` and `tools/migration/reformat_to_flat_items.py` also consume it. Don't assume every domain still emits the four-bucket shape directly — check whether the domain has migrated before writing bucket-shape-dependent tooling.

## Sig-Hash Policy System (Analysis-Side)

A second, policy-driven hash-computation path exists alongside the inline `sig_hash` every domain already computes during extraction:

- `contracts/domain_identity_keys_v2.json` (the per-domain key registry) is the source of truth.
- `tools/generate_sig_hash_policy.py` compiles it into `policies/domain_sig_hash_policies.json` (per-domain `allowed_items`/`required_items`/`hash_alg`).
- `core/sig_hash_policy.py` loads/validates that policy file; `core/sig_hash_builder.py` (`build_sig_hash_from_policy` / `apply_sig_hash_policy_to_record`) recomputes `sig_hash`/`status`/`sig_basis` from a record's flat `items` against the policy.
- `tools/run_extract_all.py`'s `sig_hash` stage (T0.5, runs after `flatten`, before `discover`) applies this to flattened rows — it does **not** run inside the live Dynamo extraction path. Domains still compute their own `sig_hash` inline via `core/record_v2.py` during extraction.
- `tools/discover_hash_policy.py` uses the same greedy/scored candidate search as join-key discovery (`tools/join_key_discovery/`) to help derive sig-hash policy candidates empirically. See `docs/hash_discovery_tooling.md`.

Treat this as the analysis-side reconstruction/audit layer for sig_hash, not (yet) a replacement for the extractor's own hash computation.

## Shape-Gating System

Shape-gating enables conditional join-key composition based on discriminator values, via a `shape_gating` block (`discriminator_key` + per-value `shape_requirements`) in a domain's policy entry. Policy lives in `policies/domain_join_key_policies.json`. See `docs/join_key_shape_gating.md` for schema details.

Currently supported via the shared `shape_gating` mechanism: `arrowheads` (discriminator `arrowhead.style`, e.g. Arrow/Heavy end tick mark) and `identity` (discriminator `identity.is_workshared`, gates `identity.revit_version_number` as additional-required when workshared).

**Not** on this mechanism despite the name: `dimension_types_*` (D-015 domain-split architecture). Shape discrimination for dimension_types now happens at the domain-split level instead — each of the 7 partitions (`dimension_types_linear`, `_angular`, `_radial`, `_diameter`, `_spot_elevation`, `_spot_coordinate`, `_spot_slope`) carries its own flat per-domain policy with `dim_type.shape` as a plain required item, not a shared `shape_gating` discriminator block. See `tests/test_dimension_types_shape_gating.py`'s module docstring.

## Context Dictionary Schema

The runner populates `ctx` for domain cross-references:

**Runner-provided**: `_collect`, `_doc_view`, `debug_vg_details`

**Domain-populated** (for downstream domain use):
- `phase_uid_to_hash` — phases → view_templates
- `phase_filter_uid_to_hash` — phase_filters → view_templates
- `view_filter_uid_to_hash` / `view_filter_uid_to_sig_hash_v2` — view_filter_definitions → view_templates
- `line_pattern_uid_to_hash` — line_patterns → object_styles, line_styles
- `object_style_row_key_to_sig_hash` — object_styles_model → view_category_overrides
- `object_style_annotation_row_key_to_sig_hash` — object_styles_annotation → view_category_overrides
- `material_uid_to_name` / `material_uid_to_class` — materials → compound_types

## Development Workflow

### Commit Message Convention
```
chore:    docs, scaffolding, non-code setup
refactor: moves/splits with NO semantic change
feat:     new domains or semantic inputs
fix:      bug fix that changes behavior
```

Every commit message MUST state "no semantic change" OR describe the semantic change.

### CHANGELOG Discipline
Log ONLY semantic changes (signature composition, ordering rules, identity rules, fail-soft behavior). Do NOT log pure refactors. `CHANGELOG.md`'s `[Unreleased]` section is actively maintained with detailed entries — read recent entries before making a change in an area to understand the last-decided behavior and its rationale.

### Domain Development Pattern

```python
# domains/example.py
from core.hashing import make_hash, safe_str
from core.canon import canon_str, S_MISSING, S_UNREADABLE
from core.record_v2 import build_record_v2, make_identity_item
from core.phase2 import phase2_sorted_items

try:
    from Autodesk.Revit.DB import ...
except ImportError:
    ... = None  # Allow non-Revit testing

def extract(doc, ctx=None):
    """Extract domain data from Revit document."""
    return {
        "hash": "<32-hex MD5 or None>",
        "count": int,
        "record_rows": [...],
        "records": [...],  # record.v2 format
        "status": "ok|degraded|blocked|failed"
    }
```

For consolidated extractors that emit multiple partitions, `extract()` returns a list of per-partition result dicts, each with its own `domain` key.

## Common Tasks

### Adding a New Domain
1. Create `domains/new_domain.py` with `extract(doc, ctx=None)` function
2. Add to import list in `runner/run_dynamo.py`
3. Register allowed keys in `contracts/domain_identity_keys_v2.json`
4. Add join-key policy in `policies/domain_join_key_policies.json` if applicable
5. Add tests in `tests/`
6. Document ordering behavior and identity rules
7. Update `DECISIONS.md` if introducing new semantic rules

### Adding a New Partition to a Consolidated Domain
1. Add record-class routing in the domain extractor's internal dispatch
2. Add a flat join-key policy entry for the new partition name
3. Register allowed keys in `contracts/domain_identity_keys_v2.json`
4. Add canonical-selector tests in `tests/test_<domain>_canonical_selectors.py`
5. Update runner if the partition needs a new `ctx` key or dependency
6. Document in `CHANGELOG.md` if the change affects hashes

### Adding Shape-Gated Properties
1. Define discriminator key and shape values in join-key policy (`shape_requirements` block)
2. Implement shape detection in domain extractor
3. Mark non-applicable properties with `q: "not_applicable"`
4. Add shape-gating tests in `tests/test_<domain>_shape_gating.py`
5. Validate with `test_join_key_policy_validation.py`

### Modifying Hash Composition
1. Document the change in `DECISIONS.md`
2. Log the semantic change in `CHANGELOG.md`
3. Update affected tests
4. Verify golden file comparisons still pass (or update them)

### Debugging Hash Mismatches
1. Check `record_rows` for per-record hash preimages
2. Verify ordering (order-sensitive vs. sorted)
3. Check for sentinel handling differences
4. Check shape-gating discriminator values if applicable
5. For extraction-side mismatches, diff `record_rows`/`identity_basis.items` directly; for analysis-side reconstruction, use `tools/discover_hash_policy.py` / `core/sig_hash_builder.py` against flattened CSV

## Analysis Pipeline

The analysis side of the codebase is separate from extraction. Exports flow through a staged pipeline:

```
Extraction (Dynamo)
  → *.details.json / *.index.json
    → tools/run_extract_all.py stage machine (flatten → sig_hash → discover → apply → ...)
      → segment/governance comparison tools
        → deterministic governance narrative
```

The primary orchestrator is `tools/run_extract_all.py`, an explicit stage machine. `docs/extract_stage_matrix.md` is the authoritative reference (source of truth is the code — `run_extract_all.py`'s `stage_names`):

| Stage | T-label | Purpose | Default in `--stages` |
|-------|---------|---------|------------------------|
| `flatten` | T0 | Emit v2.1 flatten outputs to `Results_v21/phase0_v21/` (identity-mode join fields) | ✅ |
| `sig_hash` | T0.5 | Recompute `sig_hash`/`status` from policy over flattened rows (`core/sig_hash_builder.py`) | ✅ (auto-inserted before `discover`/`apply` if selected) |
| `discover` | T1 | Explore per-domain join-key policy candidates from flatten identity items | ✅ |
| `apply` | T2 | Apply policy and overwrite flatten `phase0_records.csv` join fields | ❌ opt-in |
| `placeholders` | T2b | Generate per-domain placeholder exclusion CSVs (purgeable heuristics + `policies/placeholder_known_defaults.json`); human review required | ❌ opt-in; requires `apply` |
| `split` | — | Split detection analysis over selected domains | ❌ opt-in; requires policy-applied join keys |
| `authority` | — | v2.1 authority analysis output | ❌ opt-in; requires policy-applied join keys |
| `patterns` | — | v2.1 per-domain patterns analysis output | ❌ opt-in; requires policy-applied join keys |
| `flat_tables` | — | Write flat CSV tables (layer stacks etc.) via `export_to_flat_tables.py` | ❌ opt-in |

Stages gated on policy-applied join keys fail by default if `join_key_schema == sig_hash_as_join_key.v1` (identity-mode clustering — degraded for governance conclusions); override explicitly with `--allow-sig-hash-join-key` only for exploratory/non-governance analysis.

### Segment & governance comparison (downstream of the stage machine)

A separate layer builds comparable populations across the whole model corpus and compares them:

1. `tools/build_segment_manifest.py` — builds the full segmentation lattice (every subset of unit_system/governance_role/client/discipline/business_center/collection) from `file_metadata.csv`.
2. `tools/run_segment_orchestrator.py` — runs `patterns_analysis` then `bundle_analysis` stages per segment, in level order, writing per-segment output folders.
3. `tools/governance_manifest.py` — builds a **disjoint** partition (Enterprise / each business center / each client / each named project / Generic) directly from `file_metadata.csv`. This is intentionally separate from the segment lattice's powerset — see the file's own docstring.
4. `tools/compare_cross_segment.py` (segments) / `tools/compare_governance_populations.py` (disjoint populations) — Jaccard + containment comparisons using `join_hash` as the cross-population identity unit; bundle membership from `bundle_analysis/` is annotated on afterward.
5. `tools/generate_governance_narrative.py` — deterministic, template-driven `governance_narrative_context.md` from the CSV outputs above. No LLM in the loop. Also emits a governance evidence package (`governance_package_manifest.json`/`_health.json`/`_evidence_map.json`/`_findings.json`/`governance_brief.md`/`governance_file_inventory.json`, default on) plus static interpretation-guide/question-route docs — see `docs/governance_evidence_package.md`.
6. `tools/governance/standards_governance_report.py` — standards governance report generation.
7. `tools/archetype/` — separate DP1 (Decision Point 1) workflow that clusters cross-domain co-occurrence signals into candidate "archetypes" for human curation against `config/archetype/archetype_definitions.json`.

`tools/similarity_compare.py`-style whole-file similarity scoring is deprecated in favor of `compare_cross_segment.py` (see `docs/tools_DEPRECATED.md` for the specific correctness bugs that motivated the deprecation — historical similarity scores are not salvageable).

### Input format priority

Tools that consume export JSON MUST follow this preference order:
1. `*.details.json` — record-level, identity_items available
2. `*.index.json` — summary only; degraded semantics
3. fallback: `*.json` excluding `*.legacy.json`

Never implicitly load `*.legacy.json`. Tools that glob `*.json` without filtering are unsafe under split exports.

### Two distinct baseline concepts

**Seed-baseline** (Phase-1 only) — a labeling bias for authority framing. Set via `seed_baseline_id` in `run_config.json`. Does NOT define correctness or affect Phase-2 identity. **Do not use yet — authority has not been established.**

**Phase-2 baseline** — a comparison anchor for change analysis. Requires stable join keys and accepted authority. **Do not use yet — pairwise + population mode is correct.**

### Current operating mode

This project is in **pre-authority probe mode**:
- Use Phase-0 flattening and Phase-2 population/pairwise analysis freely
- Use Phase-1 with `domains_in_scope` populated but no `seed_baseline_id`
- Do NOT use seed-baseline or Phase-2 baselines/change-type narratives

### Phase-1 configuration

Phase-1 behavior is entirely governed by `tools/run_config.json`. If `domains_in_scope` is empty (`[]`), Phase-1 is disabled (headers-only output — this is intentional, not an error).

### Key docs for analysis work
- `docs/extract_stage_matrix.md` — current, authoritative orchestrator stage-machine reference
- `docs/hash_discovery_tooling.md` — sig-hash/join-key discovery tooling usage
- `docs/cross_segment_comparison.md` — `compare_cross_segment.py` methodology
- `docs/analysis-phases-question-map.md` — which questions each phase can answer
- `docs/V21_ANALYSIS_SCHEMA.md` — v2.1 output schema (`Results_v21/analysis_v21/`)
- `docs/governance_evidence_package.md` — `generate_governance_narrative.py`'s evidence-package layer (manifest/health/evidence-map/findings artifact inventory, authority ordering, policy/threshold profiles, interpretation guide/question routes/governance brief)
- `docs/governance_interpretation_guide.md` — stable, package-type-level interpretation guide for a governance evidence package (metric semantics, comparability rules, known bad inferences)
- `docs/governance_question_routes.md` — candidate question-route catalog (all at "candidate" maturity) for recurring governance-package questions
- `docs/tools_PHASE0_1_2_MAP.md` / `docs/tools_DEPRECATED.md` — useful for deprecation *reasoning*, but dated 2026-01-29 and reference a `tools/phase2_analysis/` package path that no longer exists on disk; don't treat their command examples as current without checking the actual file first

## Files to Read First

When working on **extraction**:
1. `INVARIANTS.md` - Non-negotiable rules
2. `DECISIONS.md` - Architectural decisions
3. `ARCHITECTURE.md` - Layered design
4. `contracts/record_contract_v2.md` - Record schema
5. `docs/join_key_shape_gating.md` - Shape-gating system
6. `REVIT_LOOKUP_DOMAIN_MAP.md` + `reference/revit_lookup/Descriptors/` - ground-truth Revit API surface per domain

When working on **analysis**:
1. `docs/extract_stage_matrix.md` - Orchestrator stage semantics (current)
2. `docs/analysis-phases-question-map.md` - Phase question map
3. `docs/cross_segment_comparison.md` / relevant tool's own module docstring - the segment/governance tools are heavily self-documenting; read the target script's top-of-file docstring before modifying it

## Key Decisions Reference

| Decision | Summary |
|----------|---------|
| D-001 | Behavior-first fingerprinting (not UI presentation) |
| D-002 | Deterministic, auditable hashes with explicit preimages |
| D-003 | `record_rows` is canonical explainability |
| D-004 | `UniqueId` restricted to element-backed identities |
| D-005 | Fail-soft is mandatory |
| D-006 | Ordering rules explicit per domain |
| D-007 | Global vs contextual domain split |
| D-008 | View templates are behavioral, not nominal |
| D-009 | Views compose templates + deltas |
| D-010 | **REVISED**: Phase names ARE included in behavioral hashes (cross-project comparability) |
| D-011 | Domain-driven architecture |
| D-012 | Markdown portability rule (no nested fenced blocks) |
| D-013 | `phase_graphics` disabled (API limitation) |
| D-014 | **COMPLETED**: Semantic (record.v2) hashing is now the only mode; legacy removed |
| D-015 | Domain family architecture — Revit system family boundary is partition criterion; consolidated extractors with internal routing |
| D-016 | VCO scope — categories 1 (template-controlled) and 2 (latent) implemented; category 3 (view-local) deferred |
| D-017 | `line_patterns` join key upgraded to scale-invariant normalized segments (structural equivalence, not absolute scale) |
| D-018 | `loaded_family_types` scoped to user-loaded families only; system families pass through unfiltered but ungoverned |
| D-019 | Governance narrative evidence-package layer, Phase 1 — package manifest/health/evidence-map JSON artifacts around `generate_governance_narrative.py`'s existing outputs |
| D-020 | Governance narrative evidence-package layer, Phase 2 — structured findings (`governance_findings.json`) with epistemic provenance (origin/fidelity/authority/limits) |
| D-021 | Governance narrative evidence-package layer, Phase 3 — policy externalization (`policies/governance/*.json`); thresholds/domain policy/onboarding rules loaded via `tools/governance_policy.py` instead of hardcoded, with defaults preserved exactly |
| D-022 | Governance narrative evidence-package layer, Phase 4 — interpretation/routing split: `docs/governance_interpretation_guide.md` (stable), `docs/governance_question_routes.md` (candidate routes), `governance_brief.md` (per-run, generated, computes nothing new) |
| D-023 | Governance narrative evidence-package layer, Phase 5 — `governance_file_inventory.json`: live `Path.glob` scan of the export directory naming CSVs this generator never reads (header/dtype/row-count only, no sample values), so an escalating reader can discover a drill-down file exists; no query/fetch mechanism added, package stays single-shot |
| D-024 | Governance narrative evidence-package layer, Phase 6 — the four files `generate_governance_narrative.py` writes no read path for (`comparison_registry.csv`, `cross_segment_file_pairs.csv`, `pattern_reuse_summary_by_domain.csv`, `project_mean_file_pair_jaccard_matrix.csv`) get full evidence-map entries instead of generic-scan treatment; `governance_evidence_map.json` grows from 33 to 35 artifacts |
| D-025 | `identity` domain expansion — reads `doc.ProjectInformation` for the first time (`project_info.*`: Number/Status/Address/Issue Date/Client/Building/Organization/IFC GUIDs, plus Stantec's `Office` shared parameter); these fields are included in `sig_hash` (hash-breaking, `sig_hash_schema` version bumped) |

`DECISIONS.md` is append-only; a couple of decision numbers (D-014, D-015) have more than one entry as the decision was revised/completed in place — the latest entry for a given number is authoritative. See `DECISIONS.md` for full rationale.

## Warnings

- NEVER change hash semantics without updating `DECISIONS.md` and `CHANGELOG.md`
- NEVER add new sentinel literals beyond the 3 approved ones
- NEVER make domains import other domains
- NEVER use `FilteredElementCollector` directly in domains (use `core/collect.py`)
- The `phase_graphics` domain is intentionally disabled - do not attempt to enable without API justification
- Shape-gating policies MUST be validated via `test_join_key_policy_validation.py`
- Phase names ARE included in hashes (D-010 revised) - this is intentional for cross-project comparison
- The D-015 domain family splits are hash-breaking — previous exports are obsolete and require full re-extraction
- Consolidated extractors emit multiple partition domains; do not add a new flat domain for what should be a partition
- `domains/graph_2024.json` / `graph_2025.json` / `graph_2026.json` are cached RevitLookup reference graphs, not part of the extraction runtime — never import them from a domain or the runner
- `tools/_archive/` holds confirmed-superseded tools; don't build new work on top of anything in it without first checking `docs/tools_DEPRECATED.md` and `CHANGELOG.md` for the replacement
- `docs/tools_PHASE0_1_2_MAP.md` and `docs/tools_DEPRECATED.md` are dated 2026-01-29 and reference a `tools/phase2_analysis/` path that no longer exists — verify any command from them against the actual file before running it
- `generate_governance_narrative.py`'s governance thresholds, excluded/passive-inheritance-risk domain lists, domain guidance text, and onboarding-interpretation thresholds live in `policies/governance/*.json` (D-021), not as Python literals — module-level constants of the same name still exist (for backward-compatible imports) but are reassigned at runtime from the loaded policy in `apply_governance_policy()`; edit the JSON, not the `_DEFAULT_*` Python fallbacks, to change actual behavior
- `core/sig_hash_builder.py` / `core/sig_hash_policy.py` are analysis-side only (the `sig_hash` stage in `run_extract_all.py`); they are not wired into the live Dynamo extraction path — don't assume changing `policies/domain_sig_hash_policies.json` changes what a domain extractor emits

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
