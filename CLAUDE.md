# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Revit Fingerprint** extracts deterministic, behavior-based fingerprints from Revit models. It identifies **what a model does**, not how it is named or presented in the UI. The system enables standards governance, drift detection, and cross-project comparison.

**Primary runtime**: Dynamo CPython3 (via `runner/run_dynamo.py`)

## Commands

```bash
# Run all unit tests
pytest tests/ -v

# Run a single test file
pytest tests/test_hashing_incremental.py

# Validate exported JSON against the record.v2 contract
FINGERPRINT_JSON_PATH=/path/to/export.json pytest tests/test_record_contract_v2.py
```

No `requirements.txt` or `pyproject.toml` exists. The only external dependency for development is `pytest` (`pip install pytest`). CI runs Python 3.9–3.12 via `.github/workflows/ci.yml`.

## Architecture

The system follows a domain-driven, layered architecture:

```
Layer 0 - Core (Pure Python)     → core/
Layer 1 - Domain Extractors      → domains/
Layer 2 - Context Builder        → core/context.py, core/collect.py
Layer 3 - Host-specific Runners  → runner/
```

**Dependency direction**: `Core → Domains → Context → Runner`

Reverse dependencies are forbidden. Domains do NOT import each other.

## Directory Structure

```
core/                   Pure Python utilities (no Revit API calls)
  hashing.py            MD5 hashing (CLR/hashlib dual-runtime)
  canon.py              Canonicalization + sentinels (<MISSING>, <UNREADABLE>, <NOT_APPLICABLE>)
  contracts.py          Contract envelopes, status rollups, bounded errors
  record_v2.py          record.v2 schema utilities & canonicalization
  phase2.py             Phase-2 join-key/join-hash helpers
  context.py            View-scoped context (ViewInfo, DocViewContext)
  deps.py               Dependency enforcement (Blocked exception, require_domain)
  rows.py               Parameter reading, unit conversion
  collect.py            FilteredElementCollector caching
  join_key_builder.py   Build join keys from policies with shape-gating
  join_key_policy.py    Load & validate join-key policies
  graphic_overrides.py  Shared helpers for graphics extraction
  features.py           Cohort-analysis feature surface
  naming.py             Document-derived naming helpers
  manifest.py           Stable manifest surface for comparison
  dimension_type_helpers.py  Shape constants, detection, and reading helpers (shared by dimension_types)
  timing_collector.py   Extraction profiling instrumentation
  vg_sig.py             VG signature helpers for view_templates

domains/                One extract(doc, ctx) function per domain (active)
  identity.py           Project metadata (NO HASH - metadata only)
  units.py              Length/area/volume format options
  object_styles.py      Object style definitions (model/annotation/analytical/imported partitions)
  line_patterns.py      Line pattern definitions
  line_styles.py        Line style definitions
  fill_patterns.py      Fill pattern definitions (drafting/model partitions)
  text_types.py         Text type definitions
  arrowheads.py         Arrowhead definitions with shape-gating
  dimension_types.py    Dimension type definitions (7 partitions: linear/angular/radial/diameter/
                          spot_elevation/spot_coordinate/spot_slope)
  phases.py             Phase inventory & sequence
  phase_filters.py      Phase filter definitions
  phase_graphics.py     Phase graphic overrides (DISABLED - API limitation, D-013)
  view_filter_definitions.py        Detailed filter rule extraction
  view_filter_applications_view_templates.py  Filter application stacks
  view_templates.py     Template definitions (5 partitions by ViewType family)
  view_category_overrides.py        VCO coordinator
  view_category_overrides_model.py      Model category override partition
  view_category_overrides_annotation.py Annotation category override partition
  materials.py          Materials domain (identity + graphics state; v1)
  compound_types.py     Compound type family (wall_types active; floor/roof/ceiling stubs)

runner/                 Host-specific entry points
  run_dynamo.py         Primary Dynamo CPython3 runner (M5 implementation)
  thin_runner.py        Lightweight wrapper for Dynamo environment control

validators/             Output validation
  record_v2.py          record.v2 schema validation

policies/               Join-key policies and alignment keys
  domain_join_key_policies.json  Per-domain join-key policies with shape-gating
  cross_domain_alignment_keys.json  Domain family registry and alignment key definitions

tools/                  Analysis & comparison utilities
  run_extract_all.py    Primary orchestrator (stage-machine: flatten→discover→apply→analyze)
  run_config.json       Phase-1 configuration (domains_in_scope, thresholds, seed_baseline_id)

  v21_emit.py           v2.1 export emitter
  v21_discover_join_policy.py   Discover join-key policy candidates
  v21_apply_join_policy.py      Apply policy to flatten outputs
  validate_v21_contract.py      Validate v2.1 contract compliance

  export_to_flat_tables.py      Phase-0: Flatten record.v2 details → CSV tables
  merge_split_exports.py        Merge split export artifacts
  details_to_csv.py             Details extraction to CSV

  phase1_domain_authority.py    Phase-1: Domain authority clustering
  phase1_population_framing.py  Phase-1: Coverage/adoption framing
  phase1_pairwise_analysis.py   Phase-1: Project-vs-project summaries

  phase2_analysis/              Phase-2 analysis package
    io.py                       IO contract (must prefer *.details.json)
    run_joinhash_label_population.py
    run_joinhash_parameter_population.py
    run_candidate_joinkey_simulation.py
    run_population_stability.py
    run_identity_collision_diagnostics.py
    run_collision_differencing.py
    run_change_type.py          ⚠ Requires Phase-2 baseline
    run_attribute_stress.py     ⚠ Requires Phase-2 baseline
    run_dimension_types_by_family.py  (probe or baseline-anchored)
    run_view_templates_joinkey_analysis.py
    run_view_category_overrides_joinkey_analysis.py
    run_text_types_candidate_joinkey_simulation.py
    split_detection.py, split_detection_element_level.py, split_detection_file_level.py
    pareto_join_keys_by_ids.py, pareto_with_splits.py
    annotate_cluster_labels.py, build_reference_standards.py
    domain_identity_contract.py, intradomain_summary.py

  join_key_discovery/           Phase-1.5: Join-key discovery (eval.py, greedy.py)
  label_synthesis/              Label synthesis and fragmentation repair
  bundle_analysis/              Placeholder/exclusion analysis
  governance/                   Standards governance reporting
  probes/                       API probes (15 domain-specific probes)

  compare_manifest.py           Diff two manifests
  pairwise_drift.py             Cross-project drift scoring
  score_drift.py                Drift score vs baseline using stable surfaces
  similarity_compare.py         Similarity comparison
  pareto_joinkey_search.py      Join-key optimization (Pareto front)
  pareto_make_shape_inputs.py   Shape-based input prep
  compute_governance_thresholds.py
  compute_synthetic_keys.py
  run_split_detection_all.py    Split detection over all domains

tests/                  pytest test suite (38+ test files + fixtures)
  test_sentinel_policy.py            Enforce only 3 allowed sentinels
  test_hashing_incremental.py        Hash determinism
  test_contracts_run_status.py       Status rollup (failed > degraded > ok)
  test_contracts_bounded_errors.py   Bounded error handling
  test_record_contract_v2.py         record.v2 schema validation
  test_record_v2_utils.py            record.v2 utility tests
  test_no_direct_filtered_element_collector_in_domains.py  Architecture enforcement
  test_deps_require_domain.py        Dependency blocking
  test_arrowheads_shape_gating.py    Arrowhead shape-gating validation
  test_dimension_types_shape_gating.py  Dimension type shape-gating
  test_join_key_policy_validation.py Join-key policy rule enforcement
  test_join_key_builder_shape_gating_dedupe.py  Join-key deduplication
  test_pareto_shape_gating.py        Pareto shape-gating analysis
  test_split_export.py               Split export functionality
  test_record_id_determinism.py      record_id generation stability
  test_timing_collector.py           Extraction profiling
  test_graphic_overrides.py          VG signature extraction
  test_collect.py                    Element collection cache behavior
  test_join_key_discovery_shape_matching.py  Shape matching in join-key discovery
  test_join_key_migration.py         Join-key policy migration validation
  test_v21_join_policy_compat.py     Version 2.1 join policy compatibility
  test_*_canonical_selectors.py      Domain-specific canonical selector tests (14 domains)
  revit/                             Revit integration harness (requires Revit)
  golden/                            Golden file comparisons

contracts/              Machine-readable contracts
  record_contract_v2.md              record.v2 schema documentation
  record_contract_v2.schema.json     JSON schema for validation
  domain_identity_keys_v2.json       Per-domain key registry with minima
  phase2_join_keys.md                Phase-2 join key specification

docs/                   Technical documentation
  join_key_shape_gating.md           Shape-gating schema extension
  SPLIT_EXPORT.md                    Split export data model
  phase2-identity-and-semantic-plan.md  Phase-2 contract design
  phase_2_join-key_discovery.md      Join-key discovery methodology
  fingerprint_hashing_rules.md       Hashing rule documentation
  tools_PHASE0_1_2_MAP.md            Tool categorization by phase
  analysis-phases-question-map.md    Analysis questions mapped to phases

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

## Shape-Gating System

Shape-gating enables conditional join-key composition based on discriminator values. Supported domains: `arrowheads` (by ArrowheadStyle) and `dimension_types_*` partitions. Policy lives in `policies/domain_join_key_policies.json`. See `docs/join_key_shape_gating.md` for schema details.

## Context Dictionary Schema

The runner populates `ctx` for domain cross-references:

**Runner-provided**: `_collect`, `_doc_view`, `debug_vg_details`

**Domain-populated** (for downstream domain use):
- `phase_uid_to_hash` — phases → view_templates
- `phase_filter_uid_to_hash` — phase_filters → view_templates
- `view_filter_uid_to_hash` / `view_filter_uid_to_sig_hash_v2` — view_filter_definitions → view_templates
- `line_pattern_uid_to_hash` — line_patterns → object_styles, line_styles
- `object_style_model_row_key_to_sig_hash` — object_styles_model → view_category_overrides
- `object_style_annotation_row_key_to_sig_hash` — object_styles_annotation → view_category_overrides

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
Log ONLY semantic changes (signature composition, ordering rules, identity rules, fail-soft behavior). Do NOT log pure refactors.

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
5. Use `tools/compare_manifest.py` for diff analysis

## Analysis Pipeline

The analysis side of the codebase is separate from extraction. Exports flow through a staged pipeline:

```
Extraction (Dynamo)
  → *.details.json / *.index.json
    → Phase 0: Flatten to CSV (export_to_flat_tables.py)
      → Phase 1: Authority/probe clustering (phase1_*.py)
      → Phase 2: Join-key & stability analysis (phase2_analysis/)
```

The primary orchestrator is `tools/run_extract_all.py`, which implements a stage machine:

| Stage | Purpose | Default |
|-------|---------|---------|
| `flatten` (T0) | Emit v2.1 flatten outputs to `Results_v21/phase0_v21/` | ✅ |
| `discover` (T1) | Explore join-key policy candidates per domain | ✅ |
| `apply` (T2) | Apply policy and overwrite join fields | ❌ opt-in |
| `placeholders` (T2b) | Generate placeholder exclusion CSVs (human review required) | ❌ opt-in |
| `split` | Split detection analysis | ❌ opt-in |
| `analyze1` / `analyze2` | Phase-1/2 analysis packets | ❌ opt-in |

### Input format priority

Tools that consume export JSON MUST follow this preference order:
1. `*.details.json` — record-level, identity_items available
2. `*.index.json` — summary only; degraded semantics
3. fallback: `*.json` excluding `*.legacy.json`

Never implicitly load `*.legacy.json`. Tools that glob `*.json` without filtering are unsafe under split exports.

### Two distinct baseline concepts

**Seed-baseline** (Phase-1 only) — a labeling bias for authority framing. Set via `seed_baseline_id` in `run_config.json`. Does NOT define correctness or affect Phase-2 identity. **Do not use yet — authority has not been established.**

**Phase-2 baseline** — a comparison anchor for change analysis (`run_change_type`, `run_attribute_stress`). Requires stable join keys and accepted authority. **Do not use yet — pairwise + population mode is correct.**

### Current operating mode

This project is in **pre-authority probe mode**:
- Use Phase-0 flattening and Phase-2 population/pairwise analysis freely
- Use Phase-1 with `domains_in_scope` populated but no `seed_baseline_id`
- Do NOT use seed-baseline, Phase-2 baselines, or change-type narratives
- Phase-2 runners that require a baseline (`run_change_type`, `run_attribute_stress`) should not be run

### Phase-1 configuration

Phase-1 behavior is entirely governed by `tools/run_config.json`. If `domains_in_scope` is empty (`[]`), Phase-1 is disabled (headers-only output — this is intentional, not an error).

### Key docs for analysis work
- `docs/tools_PHASE0_1_2_MAP.md` — authoritative per-tool reference with inputs, outputs, and commands
- `docs/analysis-phases-question-map.md` — which questions each phase can answer
- `docs/extract_stage_matrix.md` — full orchestrator stage-machine semantics
- `docs/V21_ANALYSIS_SCHEMA.md` — v2.1 output schema (`Results_v21/analysis_v21/`)

## Files to Read First

When working on **extraction**:
1. `INVARIANTS.md` - Non-negotiable rules
2. `DECISIONS.md` - Architectural decisions
3. `ARCHITECTURE.md` - Layered design
4. `contracts/record_contract_v2.md` - Record schema
5. `docs/join_key_shape_gating.md` - Shape-gating system

When working on **analysis**:
1. `docs/tools_PHASE0_1_2_MAP.md` - Per-tool reference with commands
2. `docs/analysis-phases-question-map.md` - Phase question map
3. `docs/extract_stage_matrix.md` - Orchestrator stage semantics

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
| D-014 | **COMPLETED (2026-02-10)**: Semantic (record.v2) hashing is now the only mode; legacy removed |
| D-015 | Domain family architecture — Revit system family boundary is partition criterion; consolidated extractors with internal routing |
| D-016 | VCO scope — categories 1 (template-controlled) and 2 (latent) implemented; category 3 (view-local) deferred |

See `DECISIONS.md` for full rationale.

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
